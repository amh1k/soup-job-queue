package migrations_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/amh1k/soup-job-queue/internal/httpapi"
	"github.com/amh1k/soup-job-queue/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Each test owns a schema; TEST_DATABASE_URL must point to a disposable database.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL for real PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("soup_test_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestMigrationAndReadiness(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	h := httpapi.NewHandler(func(ctx context.Context) error { return migrations.Check(ctx, pool) }, time.Second)
	status := func() int {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
		return w.Code
	}
	if status() != 503 {
		t.Fatal("unmigrated DB reported ready")
	}
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if status() != 200 {
		t.Fatal("migrated DB not ready")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO jobs (idempotency_key, request) VALUES ('retained', '{"duration_ms":1,"simulate_failure":false}')`); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&count); err != nil || count != 1 {
		t.Fatalf("data not retained: count=%d err=%v", count, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE schema_migrations SET checksum = 'tampered'"); err != nil {
		t.Fatal(err)
	}
	if status() != 503 {
		t.Fatal("checksum mismatch reported ready")
	}
	if err := migrations.Apply(ctx, pool); err == nil {
		t.Fatal("modified migration accepted")
	}
}

func TestSchemaConstraints(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, query, code string }{
		{"invalid state", `INSERT INTO jobs (idempotency_key,request,status) VALUES ('bad','{}','unknown')`, "23514"},
		{"empty key", `INSERT INTO jobs (idempotency_key,request) VALUES ('','{}')`, "23514"},
		{"key byte limit", `INSERT INTO jobs (idempotency_key,request) VALUES (repeat('é',65),'{}')`, "23514"},
		{"running without owner", `INSERT INTO jobs (idempotency_key,request,status) VALUES ('bad','{}','running')`, "23514"},
		{"queued with owner", `INSERT INTO jobs (idempotency_key,request,worker_id) VALUES ('bad','{}','worker')`, "23514"},
		{"terminal without completion", `INSERT INTO jobs (idempotency_key,request,status) VALUES ('bad','{}','succeeded')`, "23514"},
		{"queued with completion", `INSERT INTO jobs (idempotency_key,request,completed_at) VALUES ('bad','{}',now())`, "23514"},
		{"negative version", `INSERT INTO jobs (idempotency_key,request,claim_version) VALUES ('bad','{}',-1)`, "23514"},
		{"invalid attempts", `INSERT INTO jobs (idempotency_key,request,attempt_count,max_attempts) VALUES ('bad','{}',6,5)`, "23514"},
		{"orphan effect", `INSERT INTO job_effects (job_id,claim_version,result) VALUES (gen_random_uuid(),1,'{}')`, "23503"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, tc.query)
			pgErr, ok := err.(*pgconn.PgError)
			if !ok || pgErr.Code != tc.code {
				t.Fatalf("expected SQLSTATE %s, got %v", tc.code, err)
			}
		})
	}
	var id string
	if err := pool.QueryRow(ctx, `INSERT INTO jobs (idempotency_key,request) VALUES ('unique','{}') RETURNING id`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`INSERT INTO jobs (idempotency_key,request) VALUES ('unique','{}')`,
		fmt.Sprintf(`INSERT INTO job_effects (job_id,claim_version,result) VALUES ('%s',1,'{}'),('%s',1,'{}')`, id, id),
	} {
		_, err := pool.Exec(ctx, query)
		pgErr, ok := err.(*pgconn.PgError)
		if !ok || pgErr.Code != "23505" {
			t.Fatalf("expected unique violation, got %v", err)
		}
	}
	// Audit indexes by definition, rather than relying on generated names.
	var indexes string
	if err := pool.QueryRow(ctx, `SELECT string_agg(indexdef, E'\n') FROM pg_indexes WHERE schemaname=current_schema() AND tablename='jobs'`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"(created_at, id)", "lease_expires_at", "queued", "running"} {
		if !strings.Contains(indexes, part) {
			t.Fatalf("missing index component %q", part)
		}
	}
}
