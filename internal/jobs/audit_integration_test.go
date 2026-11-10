package jobs_test

import (
	"context"
	"testing"

	"github.com/amh1k/soup-job-queue/internal/audit"
	"github.com/amh1k/soup-job-queue/internal/jobs"
)

func TestIndependentAuditOnCorruptDatabase(t *testing.T) {
	store, pool := testStore(t)
	ctx := context.Background()
	input := jobs.CreateInput{DurationMS: 1}
	c := seedClaim(t, store, "audit")
	if err := store.Finish(ctx, *c, jobs.Result{}); err != nil {
		t.Fatal(err)
	}
	expected := []audit.Entry{{Key: "audit", Input: input}}
	report, err := audit.Inspect(ctx, pool, expected, nil)
	if err != nil || !report.Valid {
		t.Fatalf("valid fixture rejected: %+v %v", report, err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM job_effects"); err != nil {
		t.Fatal(err)
	}
	report, err = audit.Inspect(ctx, pool, expected, nil)
	if err != nil || report.Valid {
		t.Fatalf("missing effect undetected: %+v %v", report, err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE job_effects DROP CONSTRAINT job_effects_job_id_fkey;INSERT INTO job_effects(job_id,claim_version,result) VALUES(gen_random_uuid(),1,'{}')`); err != nil {
		t.Fatal(err)
	}
	report, err = audit.Inspect(ctx, pool, expected, nil)
	if err != nil || report.Valid {
		t.Fatalf("orphan effect undetected: %+v %v", report, err)
	}
}
