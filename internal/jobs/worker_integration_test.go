package jobs_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/amh1k/soup-job-queue/internal/jobs"
	"github.com/amh1k/soup-job-queue/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func runtimeOptions(id string) worker.Options {
	return worker.Options{ID: id, LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond, DBTimeout: 200 * time.Millisecond, PollInterval: 5 * time.Millisecond, RecoveryInterval: 20 * time.Millisecond, RecoveryBatch: 100}
}
func startRuntime(t *testing.T, pool *pgxpool.Pool, id string) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	s, err := jobs.NewStoreWithLease(pool, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(s, runtimeOptions(id), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("worker shutdown timed out")
		}
	})
	return cancel, done
}
func waitStatus(t *testing.T, s *jobs.PostgresStore, id string, status jobs.Status) jobs.Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		j, err := s.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == status {
			return j
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Millisecond):
		}
	}
	t.Fatalf("job %s did not reach %s", id, status)
	return jobs.Job{}
}
func TestWorkerLifecycle(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	success, _, err := s.Create(ctx, "worker-success", jobs.CreateInput{DurationMS: 20})
	if err != nil {
		t.Fatal(err)
	}
	failed, _, err := s.Create(ctx, "worker-failure", jobs.CreateInput{DurationMS: 20, SimulateFailure: true})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, _, err := s.Create(ctx, "worker-cancel", jobs.CreateInput{DurationMS: 1000})
	if err != nil {
		t.Fatal(err)
	}
	startRuntime(t, pool, "lifecycle")
	waitStatus(t, s, success.ID, jobs.Succeeded)
	waitStatus(t, s, failed.ID, jobs.Failed)
	waitStatus(t, s, cancelled.ID, jobs.Running)
	if _, err := s.Cancel(ctx, cancelled.ID); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, s, cancelled.ID, jobs.Cancelled)
	followup, _, err := s.Create(ctx, "after-cancellation", jobs.CreateInput{DurationMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, s, followup.ID, jobs.Succeeded)

	if effects(t, pool, success.ID) != 1 || effects(t, pool, failed.ID) != 0 || effects(t, pool, cancelled.ID) != 0 {
		t.Fatal("effect invariant violated")
	}
}

// The helper runs this package's actual worker runtime in an independent OS
// process. Only the parent integration test supplies this flag and schema URL.
func TestWorkerSubprocess(t *testing.T) {
	if os.Getenv("SOUP_WORKER_HELPER") != "1" {
		return
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(os.Getenv("SOUP_WORKER_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = os.Getenv("SOUP_WORKER_SCHEMA")
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := jobs.NewStoreWithLease(pool, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var queue worker.Store = store
	if os.Getenv("SOUP_WORKER_PAUSE_AFTER_CLAIM") == "1" {
		queue = &pauseAfterClaimStore{PostgresStore: store}
	}
	runtime, err := worker.New(queue, runtimeOptions("killed-worker"), slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Run(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestWorkerSIGKILLRecovery(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	job, _, err := s.Create(ctx, "killed-job", jobs.CreateInput{DurationMS: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	logFile, err := os.Create(filepath.Join(t.TempDir(), "killed-worker.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	cmd := exec.Command(executable, "-test.run=^TestWorkerSubprocess$")
	cmd.Env = append(os.Environ(), "SOUP_WORKER_HELPER=1", "SOUP_WORKER_DATABASE_URL="+os.Getenv("TEST_DATABASE_URL"), "SOUP_WORKER_SCHEMA="+schema)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	waitStatus(t, s, job.ID, jobs.Running)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	} // SIGKILL on Linux, matching the chaos-test failure mode.
	if err := cmd.Wait(); err == nil {
		t.Fatal("killed process exited successfully")
	}
	waited = true
	if effects(t, pool, job.ID) != 0 {
		t.Fatal("killed attempt committed an effect")
	}
	startRuntime(t, pool, "replacement")
	completed := waitStatus(t, s, job.ID, jobs.Succeeded)
	if completed.AttemptCount < 2 || effects(t, pool, job.ID) != 1 {
		t.Fatalf("invalid recovered outcome: %+v", completed)
	}
}

// A long job must retain its first claim across multiple original lease periods.
func TestWorkerRenewsLeaseDuringLongExecution(t *testing.T) {
	s, pool := testStore(t)
	job, _, err := s.Create(context.Background(), "long-heartbeat", jobs.CreateInput{DurationMS: 3500})
	if err != nil {
		t.Fatal(err)
	}
	startRuntime(t, pool, "long-running")
	first := waitStatus(t, s, job.ID, jobs.Running)
	startRuntime(t, pool, "competing-recovery")
	// Observe persisted renewal, rather than assuming a timer implies a heartbeat.
	deadline := time.Now().Add(2 * time.Second)
	renewed := false
	for time.Now().Before(deadline) {
		current, err := s.Get(context.Background(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.LeaseExpiresAt != nil && current.LeaseExpiresAt.After(*first.LeaseExpiresAt) {
			renewed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !renewed {
		t.Fatal("no persisted lease renewal observed")
	}
	completed := waitStatus(t, s, job.ID, jobs.Succeeded)
	if completed.AttemptCount != 1 || completed.ClaimVersion != first.ClaimVersion || effects(t, pool, job.ID) != 1 {
		t.Fatalf("healthy long-running job was reclaimed: %+v", completed)
	}
}

func TestPausedWorkerResumesAfterReassignment(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux SIGSTOP/SIGCONT")
	}
	s, pool := testStore(t)
	ctx := context.Background()
	job, _, err := s.Create(ctx, "paused-worker", jobs.CreateInput{DurationMS: 3000})
	if err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "paused-worker.log")
	output, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestWorkerSubprocess$")
	cmd.Env = append(os.Environ(), "SOUP_WORKER_HELPER=1", "SOUP_WORKER_DATABASE_URL="+os.Getenv("TEST_DATABASE_URL"), "SOUP_WORKER_SCHEMA="+schema, "SOUP_WORKER_PAUSE_AFTER_CLAIM=1")
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		output.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = output.Close() })
	first := waitStatus(t, s, job.ID, jobs.Running)
	// The helper stops itself after ClaimNext commits, outside any claim lock.
	stopped := false
	deadlineStop := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadlineStop) {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", cmd.Process.Pid))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "State:\tT") {
			stopped = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !stopped {
		t.Fatal("worker did not stop after committed claim")
	}
	startRuntime(t, pool, "replacement-after-pause")
	completed := waitStatus(t, s, job.ID, jobs.Succeeded)
	if completed.AttemptCount < 2 || completed.ClaimVersion <= first.ClaimVersion {
		t.Fatalf("not reassigned: %+v", completed)
	}
	if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	rejected := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "claim ownership lost") {
			rejected = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !rejected {
		t.Fatal("resumed worker did not report rejection of its stale attempt")
	}
	final, err := s.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != jobs.Succeeded || final.ClaimVersion != completed.ClaimVersion || effects(t, pool, job.ID) != 1 {
		t.Fatalf("stale worker changed committed job: %+v", final)
	}
}

// The helper stops between claiming and execution so it cannot freeze while
// holding the claim transaction's row lock. Only enabled by the parent test.
type pauseAfterClaimStore struct {
	*jobs.PostgresStore
	paused bool
}

func (s *pauseAfterClaimStore) ClaimNext(ctx context.Context, id string) (*jobs.Claim, error) {
	claim, err := s.PostgresStore.ClaimNext(ctx, id)
	if err == nil && claim != nil && !s.paused {
		s.paused = true
		if err := syscall.Kill(os.Getpid(), syscall.SIGSTOP); err != nil {
			return nil, err
		}
	}
	return claim, err
}
