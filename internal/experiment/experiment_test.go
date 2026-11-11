package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/amh1k/soup-job-queue/internal/jobs"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestLoadPlanAndPercentiles(t *testing.T) {
	o := DefaultOptions()
	o.Warmup = 0
	o.Duration = time.Second
	entries, ops := loadPlan(o)
	if len(ops) != 100 || len(entries) != 77 {
		t.Fatalf("plan sizes entries=%d ops=%d", len(entries), len(ops))
	}
	counts := map[string]int{}
	for _, op := range ops {
		counts[op.Kind]++
	}
	if counts["create"] != 50 || counts["read"] != 30 || counts["list"] != 10 || counts["cancel"] != 10 {
		t.Fatal(counts)
	}
	if percentile([]float64{5, 1, 3, 4, 2}, .95) != 5 {
		t.Fatal("wrong percentile")
	}
	a := manifest(100, 3, 20, 100, true)
	b := manifest(100, 3, 20, 100, true)
	for i := range a {
		if a[i] != b[i] {
			t.Fatal("manifest is not reproducible")
		}
	}
}
func TestLoadAssessmentDetectsOverload(t *testing.T) {
	o := DefaultOptions()
	o.Duration = 2 * time.Second
	start := time.Now()
	var rows []loadSample
	for i := range 200 {
		scheduled := start.Add(time.Duration(i) * 10 * time.Millisecond)
		rows = append(rows, loadSample{Phase: "measurement", Scheduled: scheduled, Started: scheduled, Ended: scheduled.Add(time.Millisecond)})
	}
	metrics := []sample{{Time: start, Queued: 0}, {Time: start.Add(time.Second), Queued: 0}}
	if report := assessLoad(rows, metrics, o, start, start.Add(o.Duration)); !report.Pass {
		t.Fatal(report.Failures)
	}
	rows[0].Dropped = true
	metrics[1].Queued = 100
	report := assessLoad(rows, metrics, o, start, start.Add(o.Duration))
	if report.Pass || report.Dropped != 1 || report.BacklogSlopePerSecond <= 0 {
		t.Fatal("overload hidden")
	}
}
func TestFailedPreflightPreservesEvidence(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	o := DefaultOptions()
	o.Output = t.TempDir()
	o.Jobs = 2
	if err := Run(context.Background(), "chaos", o); err == nil {
		t.Fatal("missing database accepted")
	}
	dirs, err := filepath.Glob(filepath.Join(o.Output, "chaos-*"))
	if err != nil || len(dirs) != 1 {
		t.Fatalf("run dirs: %v %v", dirs, err)
	}
	data, err := os.ReadFile(filepath.Join(dirs[0], "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary Summary
	if err := json.Unmarshal(data, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Success || summary.WorkloadStarted || summary.Error == "" {
		t.Fatalf("incorrect failed summary: %+v", summary)
	}
	if _, err := os.Stat(filepath.Join(dirs[0], "manifest.json")); err != nil {
		t.Fatal("manifest not preserved")
	}
}
func TestSupervisorKillsReplacesAndReaps(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX worker supervisor")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "worker")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 60\n"), 0755); err != nil {
		t.Fatal(err)
	}
	e, err := newEvidence(dir, "supervisor-test", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	supervisor := newSupervisor(script, "postgres://unused", 2, e)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan error, 1)
	done := make(chan error, 1)
	go func() { done <- supervisor.run(ctx, ready) }()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	if err := supervisor.killRandom(rand.New(rand.NewSource(1))); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	restarted := false
	for time.Now().Before(deadline) {
		supervisor.mu.Lock()
		for _, c := range supervisor.slots {
			if c.generation > 1 {
				restarted = true
			}
		}
		supervisor.mu.Unlock()
		if restarted {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("child processes not reaped")
	}
	if !restarted || supervisor.Kills.Load() != 1 || supervisor.live() != 0 || supervisor.Unexpected.Load() != 0 {
		t.Fatalf("supervisor: restart=%v killed=%d live=%d unexpected=%d", restarted, supervisor.Kills.Load(), supervisor.live(), supervisor.Unexpected.Load())
	}
}

func TestCatalogRejectsDifferentIDsForSameKey(t *testing.T) {
	c := &client{ids: map[string]string{}, cancelled: map[string]bool{}}
	entry := manifest(1, 1, 1, 1, false)[0]
	result := response{Status: 201, Job: jobs.Job{ID: "00000000-0000-0000-0000-000000000001", Request: entry.Input}}
	if err := c.accept(entry, result); err != nil {
		t.Fatal(err)
	}
	result.Status = 200
	if err := c.accept(entry, result); err != nil {
		t.Fatal(err)
	}
	result.Job.ID = "00000000-0000-0000-0000-000000000002"
	if err := c.accept(entry, result); err == nil {
		t.Fatal("different replay ID accepted")
	}
}

func TestEvidenceFailureCannotReportSuccess(t *testing.T) {
	for _, failure := range []string{"event", "close", "summary"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			f, err := os.Create(filepath.Join(dir, "events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			e := &evidence{dir: dir, file: f}
			switch failure {
			case "event":
				e.err = fmt.Errorf("injected event write failure")
			case "close":
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			case "summary":
				if err := os.Mkdir(filepath.Join(dir, "summary.json"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			var summary Summary
			if err := finishEvidence(e, &summary, nil); err == nil || summary.Success || summary.Error == "" {
				t.Fatalf("failure hidden: err=%v summary=%+v", err, summary)
			}
			if failure != "summary" {
				data, err := os.ReadFile(filepath.Join(dir, "summary.json"))
				if err != nil {
					t.Fatal(err)
				}
				var persisted Summary
				if err := json.Unmarshal(data, &persisted); err != nil {
					t.Fatal(err)
				}
				if persisted.Success {
					t.Fatal("persisted false pass")
				}
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

func TestCancellationResponseValidation(t *testing.T) {
	id := "00000000-0000-0000-0000-000000000001"
	for _, tc := range []struct {
		status int
		body   string
		valid  bool
	}{
		{200, `{"id":"00000000-0000-0000-0000-000000000001","status":"cancelled"}`, true},
		{409, `{}`, true}, {500, `{}`, false}, {200, `{"status":"running"}`, false},
	} {
		e := &evidence{dir: t.TempDir()}
		f, err := os.Create(filepath.Join(e.dir, "events.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		e.file = f
		c := newClient("http://unused", e)
		c.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
		})
		if got := c.cancelJob(context.Background(), "test", id); got != tc.valid {
			t.Fatalf("status=%d got %v", tc.status, got)
		}
		if err := e.close(); err != nil {
			t.Fatal(err)
		}
	}
}
