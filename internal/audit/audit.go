// Package audit checks persisted facts independently from the job store.
package audit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amh1k/soup-job-queue/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Entry struct {
	Key   string           `json:"key"`
	Input jobs.CreateInput `json:"input"`
}
type Row struct {
	MaxAttempts             int
	ID, Key                 string
	Input                   jobs.CreateInput
	Status                  jobs.Status
	Attempts                int
	Version                 int64
	Owner, Lease, Completed bool
	FailureCode             string
	Effects                 int
	EffectVersion           int64
}
type Report struct {
	Valid               bool `json:"valid"`
	Expected, Found     int
	States              map[jobs.Status]int
	Attempts, Exhausted int
	Violations          []string `json:"violations"`
}

func Check(expected []Entry, rows []Row, orphans int, cancellations map[string]bool) Report {
	report := Report{Expected: len(expected), Found: len(rows), States: map[jobs.Status]int{}, Violations: []string{}}
	bad := func(message string) { report.Violations = append(report.Violations, message) }
	wanted := map[string]jobs.CreateInput{}
	seen := map[string]bool{}
	ids := map[string]jobs.Status{}
	for _, entry := range expected {
		if _, ok := wanted[entry.Key]; ok {
			bad("duplicate manifest key: " + entry.Key)
		}
		wanted[entry.Key] = entry.Input
	}
	for _, row := range rows {
		report.States[row.Status]++
		report.Attempts += row.Attempts
		if row.MaxAttempts < 1 || row.Attempts < 0 || row.Attempts > row.MaxAttempts || row.Version < int64(row.Attempts) {
			bad(row.ID + ": invalid attempt/version bounds")
		}
		if row.FailureCode == "attempts_exhausted" {
			report.Exhausted++
			if row.Attempts != row.MaxAttempts {
				bad(row.ID + ": exhausted before attempt limit")
			}
		}
		prefix := row.ID + ": "
		input, ok := wanted[row.Key]
		if !ok {
			bad(prefix + "unexpected job key")
		}
		if seen[row.Key] {
			bad(prefix + "duplicate key")
		}
		seen[row.Key] = true
		if _, exists := ids[row.ID]; exists {
			bad(prefix + "duplicate job ID")
		}
		ids[row.ID] = row.Status
		if ok && input != row.Input {
			bad(prefix + "request differs from manifest")
		}
		if row.Owner || row.Lease {
			bad(prefix + "residual ownership")
		}
		if !row.Completed {
			bad(prefix + "missing terminal timestamp")
		}
		switch row.Status {
		case jobs.Succeeded:
			if row.Effects != 1 || row.EffectVersion != row.Version {
				bad(prefix + "success/effect mismatch")
			}
			if row.Input.SimulateFailure || row.Attempts < 1 || row.FailureCode != "" {
				bad(prefix + "inconsistent success")
			}
		case jobs.Failed:
			if row.Effects != 0 || row.Attempts < 1 {
				bad(prefix + "failed job has effect or no attempt")
			}
			if row.FailureCode != "attempts_exhausted" && (row.FailureCode != "execution_failed" || !row.Input.SimulateFailure) {
				bad(prefix + "unexpected failure reason")
			}
		case jobs.Cancelled:
			if row.Effects != 0 {
				bad(prefix + "cancelled job has effect")
			}
		default:
			bad(prefix + "nonterminal or invalid state")
		}
		if row.Effects > 1 {
			bad(prefix + "duplicate effects")
		}
	}
	for key := range wanted {
		if !seen[key] {
			bad("missing job: " + key)
		}
	}
	for id := range cancellations {
		if ids[id] != jobs.Cancelled {
			bad(id + ": acknowledged cancellation is not cancelled")
		}
	}
	if orphans != 0 {
		bad(fmt.Sprintf("orphan effects: %d", orphans))
	}
	report.Valid = len(report.Violations) == 0
	return report
}
func Inspect(ctx context.Context, pool *pgxpool.Pool, expected []Entry, cancellations map[string]bool) (Report, error) {
	// Workers are quiescent before this audit. No Store methods or status-changing
	// queries are used; an aggregate also detects duplicate effects in corrupt fixtures.
	rows, err := pool.Query(ctx, `SELECT j.id::text,j.idempotency_key,j.request,j.status,j.attempt_count,j.max_attempts,
 j.claim_version,j.worker_id IS NOT NULL,j.lease_expires_at IS NOT NULL,j.completed_at IS NOT NULL,
 coalesce(j.failure_code,''),count(e.job_id),coalesce(min(e.claim_version),0)
 FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id GROUP BY j.id ORDER BY j.id`)
	if err != nil {
		return Report{}, err
	}
	var snapshot []Row
	for rows.Next() {
		var row Row
		var data []byte
		if err := rows.Scan(&row.ID, &row.Key, &data, &row.Status, &row.Attempts, &row.MaxAttempts, &row.Version, &row.Owner, &row.Lease, &row.Completed, &row.FailureCode, &row.Effects, &row.EffectVersion); err != nil {
			rows.Close()
			return Report{}, err
		}
		if err := json.Unmarshal(data, &row.Input); err != nil {
			rows.Close()
			return Report{}, err
		}
		snapshot = append(snapshot, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return Report{}, err
	}
	var orphans int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM job_effects e LEFT JOIN jobs j ON j.id=e.job_id WHERE j.id IS NULL`).Scan(&orphans); err != nil {
		return Report{}, err
	}
	return Check(expected, snapshot, orphans, cancellations), nil
}
