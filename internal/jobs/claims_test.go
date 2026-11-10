package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/amh1k/soup-job-queue/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedClaim(t *testing.T, s *jobs.PostgresStore, key string) *jobs.Claim {
	t.Helper()
	if _, _, err := s.Create(context.Background(), key, jobs.CreateInput{DurationMS: 1}); err != nil {
		t.Fatal(err)
	}
	c, err := s.ClaimNext(context.Background(), "worker")
	if err != nil || c == nil {
		t.Fatalf("claim: %+v %v", c, err)
	}
	return c
}
func expire(t *testing.T, pool *pgxpool.Pool, id string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
}
func effects(t *testing.T, pool *pgxpool.Pool, id string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM job_effects WHERE job_id=$1", id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func TestConcurrentClaims(t *testing.T) {
	s, pool := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := range 8 {
		if _, _, err := s.Create(ctx, fmt.Sprint(i), jobs.CreateInput{DurationMS: 1}); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	claims := make(chan *jobs.Claim, 8)
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			c, err := s.ClaimNext(ctx, fmt.Sprintf("worker%d", i))
			claims <- c
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(claims)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for c := range claims {
		if c == nil || seen[c.Job.ID] {
			t.Fatal("duplicate or missing claim")
		}
		seen[c.Job.ID] = true
		if c.Version != 1 || c.Job.AttemptCount != 1 || c.Job.Status != jobs.Running {
			t.Fatalf("invalid claim %+v", c)
		}
	}
	if c, err := s.ClaimNext(ctx, "idle"); c != nil || err != nil {
		t.Fatalf("empty queue: %+v %v", c, err)
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status='running'").Scan(&n); err != nil || n != 8 {
		t.Fatalf("running %d: %v", n, err)
	}
}
func TestClaimSkipsLockedOldest(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	first, _, err := s.Create(ctx, "first", jobs.CreateInput{DurationMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := s.Create(ctx, "second", jobs.CreateInput{DurationMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", first.ID); err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	c, err := s.ClaimNext(bounded, "second-worker")
	if err != nil || c == nil || c.Job.ID != second.ID {
		t.Fatalf("did not skip locked job: %+v %v", c, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	c, err = s.ClaimNext(ctx, "first-worker")
	if err != nil || c == nil || c.Job.ID != first.ID {
		t.Fatalf("did not claim oldest: %+v %v", c, err)
	}
}
func TestRenewFencingAndDatabaseTime(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	c := seedClaim(t, s, "renew")
	// The caller's clock/lease snapshot is deliberately wrong. Database time wins.
	ancient := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	c.Job.LeaseExpiresAt = &ancient
	if err := s.Renew(ctx, *c); err != nil {
		t.Fatal(err)
	}
	var extended bool
	if err := pool.QueryRow(ctx, `SELECT lease_expires_at>clock_timestamp()+interval '8 seconds' FROM jobs WHERE id=$1`, c.Job.ID).Scan(&extended); err != nil || !extended {
		t.Fatalf("lease not renewed: %v", err)
	}
	wrong := *c
	wrong.WorkerID = "other"
	if err := s.Renew(ctx, wrong); !errors.Is(err, jobs.ErrOwnershipLost) {
		t.Fatalf("wrong owner: %v", err)
	}
	wrong = *c
	wrong.Version++
	if err := s.Finish(ctx, wrong, jobs.Result{Message: "stale"}); !errors.Is(err, jobs.ErrOwnershipLost) {
		t.Fatalf("wrong version: %v", err)
	}
	expire(t, pool, c.Job.ID)
	future := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	c.Job.LeaseExpiresAt = &future
	if err := s.Renew(ctx, *c); !errors.Is(err, jobs.ErrOwnershipLost) {
		t.Fatalf("expired renewal: %v", err)
	}
	if err := s.Finish(ctx, *c, jobs.Result{}); !errors.Is(err, jobs.ErrOwnershipLost) {
		t.Fatalf("expired completion: %v", err)
	}
	if n, err := s.RecoverExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("recovery: %d %v", n, err)
	}
	// Reusing the worker ID still cannot make an earlier attempt current.
	newer, err := s.ClaimNext(ctx, c.WorkerID)
	if err != nil || newer == nil {
		t.Fatalf("reclaim: %+v %v", newer, err)
	}
	if newer.Version <= c.Version || newer.Job.AttemptCount != 2 || !newer.Job.CreatedAt.Equal(c.Job.CreatedAt) {
		t.Fatalf("invalid reclaim: %+v", newer)
	}
	if err := s.Finish(ctx, *c, jobs.Result{}); !errors.Is(err, jobs.ErrOwnershipLost) {
		t.Fatalf("zombie completed: %v", err)
	}
	if err := s.Fail(ctx, *c, jobs.Failure{Code: "stale"}); !errors.Is(err, jobs.ErrOwnershipLost) {
		t.Fatalf("zombie failed job: %v", err)
	}
	if err := s.Finish(ctx, *newer, jobs.Result{Message: "complete"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, *newer, jobs.Result{}); !errors.Is(err, jobs.ErrOwnershipLost) {
		t.Fatalf("repeat finish: %v", err)
	}
	if n := effects(t, pool, c.Job.ID); n != 1 {
		t.Fatalf("effects=%d", n)
	}
	if _, err := s.Cancel(ctx, c.Job.ID); !errors.Is(err, jobs.ErrTerminal) {
		t.Fatalf("cancel completed: %v", err)
	}
}
func TestFinishRollbackAndInvariantViolation(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	c := seedClaim(t, s, "rollback")
	// Inject failure after effect insertion, before the terminal update commits.
	_, err := pool.Exec(ctx, `CREATE FUNCTION reject_success() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='succeeded' THEN RAISE EXCEPTION 'injected completion failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER reject_success BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION reject_success();`)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, *c, jobs.Result{}); err == nil {
		t.Fatal("injected failure accepted")
	}
	if effects(t, pool, c.Job.ID) != 0 {
		t.Fatal("partial effect committed")
	}
	j, err := s.Get(ctx, c.Job.ID)
	if err != nil || j.Status != jobs.Running {
		t.Fatalf("partial state: %+v %v", j, err)
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER reject_success ON jobs"); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, *c, jobs.Result{}); err != nil {
		t.Fatal(err)
	}
	corrupt := seedClaim(t, s, "corrupt")
	if _, err := pool.Exec(ctx, `INSERT INTO job_effects(job_id,claim_version,result) VALUES($1,$2,'{}')`, corrupt.Job.ID, corrupt.Version); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, *corrupt, jobs.Result{}); !errors.Is(err, jobs.ErrInvariantViolation) {
		t.Fatalf("duplicate hidden: %v", err)
	}
}
func TestFailureAndAttemptLimit(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	c := seedClaim(t, s, "failure")
	if err := s.Fail(ctx, *c, jobs.Failure{Code: "execution_failed", Message: "simulated failure"}); err != nil {
		t.Fatal(err)
	}
	j, err := s.Get(ctx, c.Job.ID)
	if err != nil || j.Status != jobs.Failed || j.FailureCode == nil || *j.FailureCode != "execution_failed" || j.CompletedAt == nil || effects(t, pool, j.ID) != 0 {
		t.Fatalf("failed state: %+v %v", j, err)
	}
	exhausted := seedClaim(t, s, "exhausted")
	if _, err := pool.Exec(ctx, "UPDATE jobs SET max_attempts=1 WHERE id=$1", exhausted.Job.ID); err != nil {
		t.Fatal(err)
	}
	expire(t, pool, exhausted.Job.ID)
	if n, err := s.RecoverExpired(ctx, 1); err != nil || n != 1 {
		t.Fatalf("recovery: %d %v", n, err)
	}
	j, err = s.Get(ctx, exhausted.Job.ID)
	if err != nil || j.Status != jobs.Failed || j.FailureCode == nil || *j.FailureCode != "attempts_exhausted" || j.WorkerID != nil || j.LeaseExpiresAt != nil {
		t.Fatalf("exhausted: %+v %v", j, err)
	}
	if c, err := s.ClaimNext(ctx, "idle"); err != nil || c != nil {
		t.Fatalf("reclaimed terminal job: %+v %v", c, err)
	}
}
func TestConcurrentRecoveryAndBatchBound(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	for i := range 5 {
		c := seedClaim(t, s, fmt.Sprint(i))
		expire(t, pool, c.Job.ID)
	}
	healthy := seedClaim(t, s, "healthy")
	n, err := s.RecoverExpired(ctx, 2)
	if err != nil || n != 2 {
		t.Fatalf("batch: %d %v", n, err)
	}
	var wg sync.WaitGroup
	counts := make(chan int, 4)
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); n, err := s.RecoverExpired(ctx, 2); counts <- n; errs <- err }()
	}
	wg.Wait()
	close(counts)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	total := n
	for n := range counts {
		total += n
	}
	if total != 5 {
		t.Fatalf("recovered %d times", total)
	}
	j, err := s.Get(ctx, healthy.Job.ID)
	if err != nil || j.Status != jobs.Running || j.ClaimVersion != healthy.Version {
		t.Fatalf("healthy job changed: %+v %v", j, err)
	}
}
func TestCancelFinishRace(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	for i := range 24 {
		c := seedClaim(t, s, fmt.Sprint(i))
		start := make(chan struct{})
		finish := make(chan error, 1)
		cancel := make(chan error, 1)
		go func() { <-start; finish <- s.Finish(ctx, *c, jobs.Result{Message: "done"}) }()
		go func() { <-start; _, err := s.Cancel(ctx, c.Job.ID); cancel <- err }()
		close(start)
		finishErr, cancelErr := <-finish, <-cancel
		j, err := s.Get(ctx, c.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		switch j.Status {
		case jobs.Succeeded:
			if finishErr != nil || !errors.Is(cancelErr, jobs.ErrTerminal) || effects(t, pool, j.ID) != 1 {
				t.Fatalf("inconsistent success: %v %v", finishErr, cancelErr)
			}
		case jobs.Cancelled:
			if cancelErr != nil || !errors.Is(finishErr, jobs.ErrOwnershipLost) || effects(t, pool, j.ID) != 0 {
				t.Fatalf("inconsistent cancellation: %v %v", finishErr, cancelErr)
			}
		default:
			t.Fatalf("unexpected status: %s", j.Status)
		}
	}
}

func TestLeaseExpiresWhileWaitingForLock(t *testing.T) {
	s, pool := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := seedClaim(t, s, "wait-for-lock")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `UPDATE jobs SET lease_expires_at=clock_timestamp()+interval '50 milliseconds' WHERE id=$1`, c.Job.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Renew(ctx, *c) }()
	if _, err := tx.Exec(ctx, `SELECT pg_sleep(0.1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, jobs.ErrOwnershipLost) {
		t.Fatalf("revived expired lease after lock wait: %v", err)
	}
}
func TestCancellationFencesAllWorkerWrites(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	c := seedClaim(t, s, "cancel-first")
	if _, err := s.Cancel(ctx, c.Job.ID); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{s.Renew(ctx, *c), s.Finish(ctx, *c, jobs.Result{}), s.Fail(ctx, *c, jobs.Failure{Code: "stale"})} {
		if !errors.Is(err, jobs.ErrOwnershipLost) {
			t.Fatalf("cancelled claim accepted: %v", err)
		}
	}
	if effects(t, pool, c.Job.ID) != 0 {
		t.Fatal("cancelled job has an effect")
	}
	if n, err := s.RecoverExpired(ctx, 10); err != nil || n != 0 {
		t.Fatalf("recovered cancelled job: %d %v", n, err)
	}
}
func TestRecoverySkipsLockedRows(t *testing.T) {
	s, pool := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := seedClaim(t, s, "locked-expired")
	expire(t, pool, c.Job.ID)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT id FROM jobs WHERE id=$1 FOR UPDATE`, c.Job.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := s.RecoverExpired(ctx, 10); err != nil || n != 0 {
		t.Fatalf("did not skip lock: %d %v", n, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := s.RecoverExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("did not recover after unlock: %d %v", n, err)
	}
}
