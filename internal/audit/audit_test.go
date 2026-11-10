package audit

import (
	"github.com/amh1k/soup-job-queue/internal/jobs"
	"testing"
)

func fixture() ([]Entry, []Row) {
	input := jobs.CreateInput{DurationMS: 1}
	return []Entry{{Key: "key", Input: input}}, []Row{{ID: "id", Key: "key", Input: input, Status: jobs.Succeeded, Attempts: 1, MaxAttempts: 5, Version: 1, EffectVersion: 1, Effects: 1, Completed: true}}
}
func TestAuditDetectsCorruption(t *testing.T) {
	for name, corrupt := range map[string]func(*Row){
		"missing effect": func(r *Row) { r.Effects = 0 }, "duplicate effects": func(r *Row) { r.Effects = 2 }, "stale effect": func(r *Row) { r.EffectVersion = 0 },
		"residual owner": func(r *Row) { r.Owner = true }, "residual lease": func(r *Row) { r.Lease = true }, "missing completion": func(r *Row) { r.Completed = false },
		"stuck running": func(r *Row) { r.Status = jobs.Running }, "unknown state": func(r *Row) { r.Status = "invalid" },
		"wrong input": func(r *Row) { r.Input.DurationMS = 2 }, "unexpected job": func(r *Row) { r.Key = "unknown" },
		"cancelled with effect": func(r *Row) { r.Status = jobs.Cancelled }, "failed with effect": func(r *Row) { r.Status = jobs.Failed },
		"no execution attempt": func(r *Row) { r.Attempts = 0 }, "success despite requested failure": func(r *Row) { r.Input.SimulateFailure = true },
	} {
		t.Run(name, func(t *testing.T) {
			expected, rows := fixture()
			corrupt(&rows[0])
			if Check(expected, rows, 0, nil).Valid {
				t.Fatal("corruption not detected")
			}
		})
	}
	expected, rows := fixture()
	if !Check(expected, rows, 0, nil).Valid {
		t.Fatal("valid success rejected")
	}
	if Check(expected, nil, 0, nil).Valid {
		t.Fatal("missing job not detected")
	}
	if Check(expected, append(rows, rows[0]), 0, nil).Valid {
		t.Fatal("duplicate job not detected")
	}
	if Check(expected, rows, 1, nil).Valid {
		t.Fatal("orphan effect not detected")
	}
	if Check(expected, rows, 0, map[string]bool{"id": true}).Valid {
		t.Fatal("cancellation inconsistency not detected")
	}
}
func TestValidFailureAndCancellation(t *testing.T) {
	expected, rows := fixture()
	rows[0].Status = jobs.Cancelled
	rows[0].Effects = 0
	if !Check(expected, rows, 0, map[string]bool{"id": true}).Valid {
		t.Fatal("valid cancellation rejected")
	}
	rows[0].Status = jobs.Failed
	rows[0].FailureCode = "attempts_exhausted"
	rows[0].MaxAttempts = 1
	if !Check(expected, rows, 0, nil).Valid {
		t.Fatal("valid exhausted job rejected")
	}
	rows[0].FailureCode = "execution_failed"
	rows[0].Input.SimulateFailure = true
	expected[0].Input.SimulateFailure = true
	if !Check(expected, rows, 0, nil).Valid {
		t.Fatal("valid simulated failure rejected")
	}
}
