package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amh1k/soup-job-queue/internal/jobs"
)

type fakeStore struct {
	claim   func(context.Context, string) (*jobs.Claim, error)
	renew   func(context.Context, jobs.Claim) error
	finish  func(context.Context, jobs.Claim, jobs.Result) error
	fail    func(context.Context, jobs.Claim, jobs.Failure) error
	recover func(context.Context, int) (int, error)
	get     func(context.Context, string) (jobs.Job, error)
}

func (s fakeStore) ClaimNext(c context.Context, id string) (*jobs.Claim, error) {
	if s.claim != nil {
		return s.claim(c, id)
	}
	return nil, nil
}
func (s fakeStore) Renew(c context.Context, j jobs.Claim) error {
	if s.renew != nil {
		return s.renew(c, j)
	}
	return nil
}
func (s fakeStore) Finish(c context.Context, j jobs.Claim, r jobs.Result) error {
	if s.finish != nil {
		return s.finish(c, j, r)
	}
	return nil
}
func (s fakeStore) Fail(c context.Context, j jobs.Claim, r jobs.Failure) error {
	if s.fail != nil {
		return s.fail(c, j, r)
	}
	return nil
}
func (s fakeStore) RecoverExpired(c context.Context, n int) (int, error) {
	if s.recover != nil {
		return s.recover(c, n)
	}
	return 0, nil
}
func (s fakeStore) Get(c context.Context, id string) (jobs.Job, error) {
	if s.get != nil {
		return s.get(c, id)
	}
	return jobs.Job{}, jobs.ErrNotFound
}
func options() Options {
	return Options{ID: "test-worker", LeaseDuration: time.Second, HeartbeatInterval: 5 * time.Millisecond, PollInterval: time.Millisecond, RecoveryInterval: 5 * time.Millisecond, DBTimeout: 20 * time.Millisecond, RecoveryBatch: 10}
}
func claim() *jobs.Claim {
	return &jobs.Claim{Job: jobs.Job{ID: "00000000-0000-0000-0000-000000000001", AttemptCount: 1, Request: jobs.CreateInput{DurationMS: 50}}, WorkerID: "test-worker", Version: 1}
}
func once(c *jobs.Claim) func(context.Context, string) (*jobs.Claim, error) {
	var n atomic.Int32
	return func(context.Context, string) (*jobs.Claim, error) {
		if n.Add(1) == 1 {
			return c, nil
		}
		return nil, nil
	}
}
func start(t *testing.T, s Store) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	w, err := New(s, options(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	return cancel, done
}
func stopped(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop")
	}
}
func TestSuccessHeartbeatsAndRecovery(t *testing.T) {
	var renewed, recovered atomic.Int32
	finished := make(chan struct{}, 1)
	s := fakeStore{claim: once(claim()), renew: func(ctx context.Context, _ jobs.Claim) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("renew missing deadline")
		}
		renewed.Add(1)
		return nil
	}, recover: func(ctx context.Context, n int) (int, error) { recovered.Add(1); return 0, nil }, finish: func(_ context.Context, _ jobs.Claim, result jobs.Result) error {
		if result.Message == "" {
			t.Error("missing result")
		}
		finished <- struct{}{}
		return nil
	}}
	cancel, done := start(t, s)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("job not finished")
	}
	cancel()
	stopped(t, done)
	if renewed.Load() == 0 || recovered.Load() == 0 {
		t.Fatalf("renewed=%d recovered=%d", renewed.Load(), recovered.Load())
	}
}
func TestSimulatedFailure(t *testing.T) {
	c := claim()
	c.Job.Request.SimulateFailure = true
	failed := make(chan jobs.Failure, 1)
	s := fakeStore{claim: once(c), finish: func(context.Context, jobs.Claim, jobs.Result) error { t.Error("failed job finished"); return nil }, fail: func(_ context.Context, _ jobs.Claim, f jobs.Failure) error { failed <- f; return nil }}
	cancel, done := start(t, s)
	select {
	case f := <-failed:
		if f.Code != "execution_failed" {
			t.Fatal(f)
		}
	case <-time.After(time.Second):
		t.Fatal("not failed")
	}
	cancel()
	stopped(t, done)
}
func TestHeartbeatFailureAbandonsAttempt(t *testing.T) {
	for _, failure := range []error{jobs.ErrOwnershipLost, errors.New("database unavailable"), context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			renewed := make(chan struct{}, 1)
			var writes atomic.Int32
			s := fakeStore{claim: once(claim()), renew: func(context.Context, jobs.Claim) error { renewed <- struct{}{}; return failure }, finish: func(context.Context, jobs.Claim, jobs.Result) error { writes.Add(1); return nil }, fail: func(context.Context, jobs.Claim, jobs.Failure) error { writes.Add(1); return nil }}
			cancel, done := start(t, s)
			select {
			case <-renewed:
			case <-time.After(time.Second):
				t.Fatal("no heartbeat")
			}
			cancel()
			stopped(t, done)
			if writes.Load() != 0 {
				t.Fatal("abandoned attempt wrote a terminal state")
			}
		})
	}
}
func TestShutdownAbandonsRunningAttempt(t *testing.T) {
	c := claim()
	c.Job.Request.DurationMS = 5000
	claimed := make(chan struct{})
	var writes atomic.Int32
	s := fakeStore{claim: func(context.Context, string) (*jobs.Claim, error) { close(claimed); return c, nil }, finish: func(context.Context, jobs.Claim, jobs.Result) error { writes.Add(1); return nil }}
	cancel, done := start(t, s)
	<-claimed
	cancel()
	stopped(t, done)
	if writes.Load() != 0 {
		t.Fatal("shutdown completed unfinished work")
	}
}
func TestTransientClaimAndRecoveryFailures(t *testing.T) {
	var calls, scans atomic.Int32
	finished := make(chan struct{}, 1)
	s := fakeStore{claim: func(context.Context, string) (*jobs.Claim, error) {
		switch calls.Add(1) {
		case 1:
			return nil, errors.New("database unavailable")
		case 2:
			return claim(), nil
		}
		return nil, nil
	}, recover: func(context.Context, int) (int, error) {
		if scans.Add(1) == 1 {
			return 0, errors.New("database unavailable")
		}
		return 1, nil
	}, finish: func(context.Context, jobs.Claim, jobs.Result) error { finished <- struct{}{}; return nil }}
	cancel, done := start(t, s)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("worker did not recover")
	}
	cancel()
	stopped(t, done)
	if calls.Load() < 2 || scans.Load() < 2 {
		t.Fatal("operations not retried")
	}
}
func TestUncertainCompletionReconcilesWithoutRepeatingEffect(t *testing.T) {
	var writes atomic.Int32
	read := make(chan struct{}, 1)
	s := fakeStore{claim: once(claim()), finish: func(context.Context, jobs.Claim, jobs.Result) error { writes.Add(1); return jobs.ErrCommitUnknown }, get: func(ctx context.Context, id string) (jobs.Job, error) {
		if ctx.Err() != nil {
			t.Error("read reused cancelled context")
		}
		read <- struct{}{}
		return jobs.Job{ID: id, Status: jobs.Succeeded, ClaimVersion: 1}, nil
	}}
	cancel, done := start(t, s)
	select {
	case <-read:
	case <-time.After(time.Second):
		t.Fatal("no reconciliation")
	}
	cancel()
	stopped(t, done)
	if writes.Load() != 1 {
		t.Fatalf("effect attempts=%d", writes.Load())
	}
}
func TestCompletionErrorDoesNotBecomeExecutionFailure(t *testing.T) {
	finished := make(chan struct{}, 1)
	var failures atomic.Int32
	s := fakeStore{claim: once(claim()), finish: func(context.Context, jobs.Claim, jobs.Result) error {
		finished <- struct{}{}
		return errors.New("connection lost")
	}, fail: func(context.Context, jobs.Claim, jobs.Failure) error { failures.Add(1); return nil }}
	cancel, done := start(t, s)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("no completion")
	}
	cancel()
	stopped(t, done)
	if failures.Load() != 0 {
		t.Fatal("infrastructure error failed the job")
	}
}
func TestInvariantFailureStopsWorker(t *testing.T) {
	s := fakeStore{claim: once(claim()), finish: func(context.Context, jobs.Claim, jobs.Result) error { return jobs.ErrInvariantViolation }}
	_, done := start(t, s)
	select {
	case err := <-done:
		if !errors.Is(err, jobs.ErrInvariantViolation) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker kept running")
	}
}
func TestOptionsValidation(t *testing.T) {
	for _, change := range []func(*Options){func(o *Options) { o.ID = "" }, func(o *Options) { o.LeaseDuration = 0 }, func(o *Options) { o.HeartbeatInterval = o.LeaseDuration }, func(o *Options) { o.DBTimeout = o.LeaseDuration }, func(o *Options) { o.PollInterval = 0 }, func(o *Options) { o.RecoveryInterval = 0 }, func(o *Options) { o.RecoveryBatch = 1001 }} {
		o := options()
		change(&o)
		if _, err := New(fakeStore{}, o, nil); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
}
