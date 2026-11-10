package jobs_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
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
	runtime, err := worker.New(store, runtimeOptions("killed-worker"), slog.New(slog.NewJSONHandler(os.Stdout, nil)))
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
