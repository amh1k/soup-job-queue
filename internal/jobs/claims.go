package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DefaultLeaseDuration = 10 * time.Second

// NewStoreWithLease configures lease duration for worker processes. All lease
// deadlines and validity checks use the database clock, never worker wall time.
func NewStoreWithLease(pool *pgxpool.Pool, lease time.Duration) (*PostgresStore, error) {
	if lease < time.Microsecond || lease > 24*time.Hour {
		return nil, ErrInvalidInput
	}
	return &PostgresStore{pool: pool, lease: lease}, nil
}

const claimSQL = `UPDATE jobs SET status='running',worker_id=$1,
 lease_expires_at=clock_timestamp()+($2::bigint * interval '1 microsecond'),
 claim_version=claim_version+1,attempt_count=attempt_count+1,
 updated_at=clock_timestamp(),failure_code=NULL,failure_message=NULL
 WHERE id=(SELECT id FROM jobs WHERE status='queued' AND attempt_count<max_attempts
 ORDER BY created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING ` + columns

// ClaimNext atomically claims the oldest unlocked job. An empty queue returns
// (nil,nil). The statement's implicit transaction ends before execution starts.
func (s *PostgresStore) ClaimNext(ctx context.Context, workerID string) (*Claim, error) {
	if strings.TrimSpace(workerID) == "" || len(workerID) > 128 {
		return nil, ErrInvalidInput
	}
	j, err := scanJob(s.pool.QueryRow(ctx, claimSQL, workerID, s.lease.Microseconds()))
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &Claim{Job: j, WorkerID: workerID, Version: j.ClaimVersion}, nil
}

func validClaim(claim Claim) bool {
	return ValidID(claim.Job.ID) && strings.TrimSpace(claim.WorkerID) != "" && len(claim.WorkerID) <= 128 && claim.Version > 0
}

// withClaim serializes against cancellation/recovery. Validate actual database
// time AFTER acquiring the lock: waiting for a lock may outlast the old lease.
// Once validated, this row lock reserves finalization until commit/rollback.
func (s *PostgresStore) withClaim(ctx context.Context, claim Claim, action func(pgx.Tx) error) error {
	if !validClaim(claim) {
		return ErrInvalidInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx)
	j, err := scanJob(tx.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1 FOR UPDATE`, claim.Job.ID))
	if errors.Is(err, ErrNotFound) {
		return ErrOwnershipLost
	}
	if err != nil {
		return err
	}
	if j.Status != Running || j.WorkerID == nil || *j.WorkerID != claim.WorkerID || j.ClaimVersion != claim.Version {
		return ErrOwnershipLost
	}
	var active bool
	if err := tx.QueryRow(ctx, `SELECT lease_expires_at>clock_timestamp() FROM jobs WHERE id=$1`, j.ID).Scan(&active); err != nil {
		return err
	}
	if !active {
		return ErrOwnershipLost
	}
	if err := action(tx); err != nil {
		return err
	}
	return commit(ctx, tx)
}

const renewSQL = `UPDATE jobs SET lease_expires_at=clock_timestamp()+($2::bigint * interval '1 microsecond'),
 updated_at=clock_timestamp() WHERE id=$1`

func (s *PostgresStore) Renew(ctx context.Context, claim Claim) error {
	return s.withClaim(ctx, claim, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, renewSQL, claim.Job.ID, s.lease.Microseconds())
		return err
	})
}

const effectSQL = `INSERT INTO job_effects(job_id,claim_version,result) VALUES($1,$2,$3)`
const succeedSQL = `UPDATE jobs SET status='succeeded',worker_id=NULL,lease_expires_at=NULL,
 completed_at=clock_timestamp(),updated_at=clock_timestamp(),failure_code=NULL,failure_message=NULL WHERE id=$1`

// Finish commits the simulated effect and succeeded state together. Repeating a
// successful Finish is rejected as ownership lost; Get can reconcile its state.
func (s *PostgresStore) Finish(ctx context.Context, claim Claim, result Result) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return s.withClaim(ctx, claim, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, effectSQL, claim.Job.ID, claim.Version, data); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return fmt.Errorf("%w: duplicate effect", ErrInvariantViolation)
			}
			return err
		}
		_, err := tx.Exec(ctx, succeedSQL, claim.Job.ID)
		return err
	})
}

const failSQL = `UPDATE jobs SET status='failed',worker_id=NULL,lease_expires_at=NULL,
 completed_at=clock_timestamp(),updated_at=clock_timestamp(),failure_code=$2,failure_message=$3 WHERE id=$1`

// Fail represents a terminal execution error. Infrastructure failures should
// abandon the attempt and let its lease expire rather than fail a healthy job.
func (s *PostgresStore) Fail(ctx context.Context, claim Claim, reason Failure) error {
	if strings.TrimSpace(reason.Code) == "" || len(reason.Code) > 128 || len(reason.Message) > 2048 {
		return ErrInvalidInput
	}
	return s.withClaim(ctx, claim, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, failSQL, claim.Job.ID, reason.Code, reason.Message)
		return err
	})
}

const expiredSQL = `SELECT id::text FROM jobs WHERE status='running' AND lease_expires_at<=clock_timestamp()
 ORDER BY lease_expires_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`
const recoverSQL = `UPDATE jobs SET
 status=CASE WHEN attempt_count>=max_attempts THEN 'failed' ELSE 'queued' END,
 completed_at=CASE WHEN attempt_count>=max_attempts THEN clock_timestamp() ELSE NULL END,
 failure_code=CASE WHEN attempt_count>=max_attempts THEN 'attempts_exhausted' ELSE NULL END,
 failure_message=CASE WHEN attempt_count>=max_attempts THEN 'Worker lease expired at the attempt limit' ELSE NULL END,
 worker_id=NULL,lease_expires_at=NULL,claim_version=claim_version+1,updated_at=clock_timestamp()
 WHERE id=$1 AND status='running' AND lease_expires_at<=clock_timestamp()`

// RecoverExpired locks a bounded batch and rechecks expiry before mutation.
// It leaves healthy leases alone and never resets the original creation time.
func (s *PostgresStore) RecoverExpired(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 1000 {
		return 0, ErrInvalidInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, expiredSQL, limit)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, id := range ids {
		tag, err := tx.Exec(ctx, recoverSQL, id)
		if err != nil {
			return 0, err
		}
		count += int(tag.RowsAffected())
	}
	if err := commit(ctx, tx); err != nil {
		return 0, err
	}
	return count, nil
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// A disconnected COMMIT may have reached PostgreSQL. Keep this distinguishable
// from a known rollback: reconnect and inspect persisted state before acting.
func commit(ctx context.Context, tx pgx.Tx) error {
	err := tx.Commit(ctx)
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.Is(err, pgx.ErrTxCommitRollback) || errors.As(err, &pgErr) || pgconn.SafeToRetry(err) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrCommitUnknown, err)
}
