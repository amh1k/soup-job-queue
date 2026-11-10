package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/amh1k/soup-job-queue/internal/httpapi"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amh1k/soup-job-queue/internal/jobs"
	"github.com/amh1k/soup-job-queue/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t *testing.T) (*jobs.PostgresStore, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL for PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("soup_jobs_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.ConnConfig.ConnectTimeout = 3 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return jobs.NewStore(pool), pool
}
func TestConcurrentIdempotentCreation(t *testing.T) {
	store, pool := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const n = 100
	results := make(chan jobs.Job, n)
	errs := make(chan error, n)
	created := make(chan bool, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			j, fresh, err := store.Create(ctx, "shared", jobs.CreateInput{DurationMS: 1})
			results <- j
			created <- fresh
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	close(created)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var id string
	for j := range results {
		if id == "" {
			id = j.ID
		}
		if j.ID != id {
			t.Fatal("same key created multiple IDs")
		}
	}
	freshCount := 0
	for fresh := range created {
		if fresh {
			freshCount++
		}
	}
	if freshCount != 1 {
		t.Fatalf("created %d times", freshCount)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if _, _, err := store.Create(ctx, "shared", jobs.CreateInput{DurationMS: 2}); !errors.Is(err, jobs.ErrIdempotencyConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	if _, err := store.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	j, fresh, err := store.Create(ctx, "shared", jobs.CreateInput{DurationMS: 1})
	if err != nil || fresh || j.ID != id || j.Status != jobs.Cancelled {
		t.Fatalf("cancelled retry: %+v fresh=%v err=%v", j, fresh, err)
	}
}
func TestPaginationWithEqualTimestamps(t *testing.T) {
	store, pool := testStore(t)
	ctx := context.Background()
	for i := range 7 {
		if _, _, err := store.Create(ctx, fmt.Sprintf("key%d", i), jobs.CreateInput{DurationMS: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET created_at='2026-01-01T00:00:00Z'"); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	var cursor *jobs.Cursor
	var previous string
	for {
		page, err := store.List(ctx, 2, cursor, jobs.Queued)
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range page.Jobs {
			if seen[j.ID] || j.ID <= previous {
				t.Fatal("duplicate or unordered job")
			}
			seen[j.ID] = true
			previous = j.ID
		}
		cursor = page.NextCursor
		if cursor == nil {
			break
		}
	}
	if len(seen) != 7 {
		t.Fatalf("listed %d", len(seen))
	}
	empty, err := store.List(ctx, 100, nil, jobs.Failed)
	if err != nil || len(empty.Jobs) != 0 || empty.NextCursor != nil {
		t.Fatalf("empty page: %+v %v", empty, err)
	}
	page, err := store.List(ctx, 1, nil, jobs.Queued)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.List(ctx, 1, page.NextCursor, jobs.Cancelled); !errors.Is(err, jobs.ErrInvalidInput) {
		t.Fatal("accepted changed cursor filter")
	}
}
func TestCancellationAndRead(t *testing.T) {
	store, pool := testStore(t)
	ctx := context.Background()
	j, _, err := store.Create(ctx, "running", jobs.CreateInput{DurationMS: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE jobs SET status='running',worker_id='worker',lease_expires_at=now()+interval '10 seconds',claim_version=1,attempt_count=1 WHERE id=$1`, j.ID); err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.Cancel(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != jobs.Cancelled || cancelled.CompletedAt == nil || cancelled.WorkerID != nil || cancelled.LeaseExpiresAt != nil || cancelled.ClaimVersion != 2 {
		t.Fatalf("invalid cancelled state: %+v", cancelled)
	}
	again, err := store.Cancel(ctx, j.ID)
	if err != nil || !again.CompletedAt.Equal(*cancelled.CompletedAt) || again.ClaimVersion != cancelled.ClaimVersion {
		t.Fatalf("repeat changed state: %+v %v", again, err)
	}
	got, err := store.Get(ctx, j.ID)
	if err != nil || got.Status != jobs.Cancelled {
		t.Fatalf("get: %+v %v", got, err)
	}
	terminal, _, err := store.Create(ctx, "terminal", jobs.CreateInput{DurationMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE jobs SET status='failed',completed_at=now() WHERE id=$1`, terminal.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Cancel(ctx, terminal.ID); !errors.Is(err, jobs.ErrTerminal) {
		t.Fatalf("terminal cancellation: %v", err)
	}
	if _, err := store.Cancel(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, jobs.ErrNotFound) {
		t.Fatalf("missing cancellation: %v", err)
	}
}

// Discarding the first response models a client that cannot know whether its
// POST committed. Retrying omitted/explicit defaults must recover the same job.
func TestHTTPRetryAfterLostResponse(t *testing.T) {
	store, pool := testStore(t)
	handler := httpapi.NewAPIHandler(store, func(context.Context) error { return nil }, 3*time.Second)
	post := func(body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/jobs", strings.NewReader(body))
		r.Header.Set("Idempotency-Key", "lost-response")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if w := post(`{"duration_ms":1}`); w.Code != 201 {
		t.Fatalf("initial POST: %d %s", w.Code, w.Body.String())
	}
	var id string
	if err := pool.QueryRow(context.Background(), "SELECT id::text FROM jobs WHERE idempotency_key='lost-response'").Scan(&id); err != nil {
		t.Fatal(err)
	}
	w := post(`{"simulate_failure":false,"duration_ms":1}`)
	var got jobs.Job
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || got.ID != id {
		t.Fatalf("retry: %d %+v", w.Code, got)
	}
	if w := post(`{"duration_ms":2}`); w.Code != 409 {
		t.Fatalf("mismatch: %d", w.Code)
	}
}
