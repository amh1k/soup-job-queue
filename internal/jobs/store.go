package jobs

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *PostgresStore { return &PostgresStore{pool: pool} }

var _ Store = (*PostgresStore)(nil)

const columns = `id::text, idempotency_key, request, status, created_at, updated_at,
 completed_at, worker_id, lease_expires_at, claim_version, attempt_count,
 max_attempts, failure_code, failure_message`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var request []byte
	err := row.Scan(&j.ID, &j.IdempotencyKey, &request, &j.Status, &j.CreatedAt,
		&j.UpdatedAt, &j.CompletedAt, &j.WorkerID, &j.LeaseExpiresAt, &j.ClaimVersion,
		&j.AttemptCount, &j.MaxAttempts, &j.FailureCode, &j.FailureMessage)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, err
	}
	if err := json.Unmarshal(request, &j.Request); err != nil {
		return Job{}, fmt.Errorf("decode persisted job: %w", err)
	}
	return j, nil
}

func ValidID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	return err == nil && len(decoded) == 16
}
func ValidStatus(status Status) bool {
	switch status {
	case Queued, Running, Succeeded, Failed, Cancelled:
		return true
	}
	return false
}

func (s *PostgresStore) Create(ctx context.Context, key string, input CreateInput) (Job, bool, error) {
	if strings.TrimSpace(key) == "" || len(key) > 128 || input.DurationMS < 1 || input.DurationMS > 5000 {
		return Job{}, false, ErrInvalidInput
	}
	request, err := json.Marshal(input)
	if err != nil {
		return Job{}, false, err
	}
	j, err := scanJob(s.pool.QueryRow(ctx, `INSERT INTO jobs (idempotency_key,request)
 VALUES ($1,$2) ON CONFLICT (idempotency_key) DO NOTHING RETURNING `+columns, key, request))
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Job{}, false, err
	}
	// A separate READ COMMITTED statement sees the winner of a concurrent insert.
	j, err = scanJob(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE idempotency_key=$1`, key))
	if err != nil {
		return Job{}, false, err
	}
	if j.Request != input {
		return Job{}, false, ErrIdempotencyConflict
	}
	return j, false, nil
}
func (s *PostgresStore) Get(ctx context.Context, id string) (Job, error) {
	if !ValidID(id) {
		return Job{}, ErrInvalidInput
	}
	return scanJob(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1`, id))
}
func (s *PostgresStore) List(ctx context.Context, limit int, cursor *Cursor, status Status) (Page, error) {
	if limit < 1 || limit > 100 || (status != "" && !ValidStatus(status)) {
		return Page{}, ErrInvalidInput
	}
	query := `SELECT ` + columns + ` FROM jobs WHERE true`
	args := []any{}
	if status != "" {
		args = append(args, status)
		query += fmt.Sprintf(" AND status=$%d", len(args))
	}
	if cursor != nil {
		if cursor.Status != status || !ValidID(cursor.ID) || cursor.CreatedAt.IsZero() {
			return Page{}, ErrInvalidInput
		}
		args = append(args, cursor.CreatedAt, cursor.ID)
		query += fmt.Sprintf(" AND (created_at,id)>($%d,$%d::uuid)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" ORDER BY created_at,id LIMIT $%d", len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()
	page := Page{Jobs: []Job{}}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return Page{}, err
		}
		page.Jobs = append(page.Jobs, j)
	}
	if err := rows.Err(); err != nil {
		return Page{}, err
	}
	if len(page.Jobs) > limit {
		page.Jobs = page.Jobs[:limit]
		last := page.Jobs[limit-1]
		page.NextCursor = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID, Status: status}
	}
	return page, nil
}
func (s *PostgresStore) Cancel(ctx context.Context, id string) (Job, error) {
	if !ValidID(id) {
		return Job{}, ErrInvalidInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Job{}, err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()
	j, err := scanJob(tx.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1 FOR UPDATE`, id))
	if err != nil {
		return Job{}, err
	}
	if j.Status == Cancelled {
		return j, nil
	}
	if j.Status == Succeeded || j.Status == Failed {
		return Job{}, ErrTerminal
	}
	j, err = scanJob(tx.QueryRow(ctx, `UPDATE jobs SET status='cancelled', worker_id=NULL,
 lease_expires_at=NULL, claim_version=claim_version+1, updated_at=clock_timestamp(),
 completed_at=clock_timestamp() WHERE id=$1 RETURNING `+columns, id))
	if err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return j, nil
}
