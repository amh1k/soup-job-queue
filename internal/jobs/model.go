package jobs

import (
	"context"
	"errors"
	"time"
)

type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Cancelled Status = "cancelled"
)

var (
	ErrNotFound            = errors.New("job not found")
	ErrIdempotencyConflict = errors.New("idempotency key used with different input")
	ErrTerminal            = errors.New("job already completed")
	ErrOwnershipLost       = errors.New("job claim is no longer current")
	ErrInvalidInput        = errors.New("invalid input")
	ErrInvariantViolation  = errors.New("job invariant violated")
	ErrCommitUnknown       = errors.New("commit outcome unknown; inspect persisted state before retrying")
)

// IDs are UUID strings, validated at the HTTP boundary in the job API task.
type CreateInput struct {
	DurationMS      int  `json:"duration_ms"`
	SimulateFailure bool `json:"simulate_failure"`
}

type Job struct {
	ID             string      `json:"id"`
	IdempotencyKey string      `json:"-"`
	Request        CreateInput `json:"request"`
	Status         Status      `json:"status"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
	CompletedAt    *time.Time  `json:"completed_at,omitempty"`
	WorkerID       *string     `json:"-"`
	LeaseExpiresAt *time.Time  `json:"-"`
	ClaimVersion   int64       `json:"-"`
	AttemptCount   int         `json:"attempt_count"`
	MaxAttempts    int         `json:"max_attempts"`
	FailureCode    *string     `json:"failure_code,omitempty"`
	FailureMessage *string     `json:"failure_message,omitempty"`
}

type Claim struct {
	Job      Job
	WorkerID string
	Version  int64
}

type Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
	Status    Status    `json:"status"`
}

type Page struct {
	Jobs       []Job
	NextCursor *Cursor
}

// Store defines the job API persistence boundary.
// Create returns newlyCreated=true only for the first insertion.
// List uses a nil cursor for the first page and an empty status for all states.
type Store interface {
	Create(ctx context.Context, key string, input CreateInput) (Job, bool, error)
	Get(ctx context.Context, id string) (Job, error)
	List(ctx context.Context, limit int, cursor *Cursor, status Status) (Page, error)
	Cancel(ctx context.Context, id string) (Job, error)
}

// Result is the simulated execution output stored as the job's single effect.
type Result struct {
	Message string `json:"message"`
}
type Failure struct {
	Code    string
	Message string
}

// Queue is the persistence interface used by the worker runtime.
type Queue interface {
	ClaimNext(context.Context, string) (*Claim, error)
	Renew(context.Context, Claim) error
	Finish(context.Context, Claim, Result) error
	Fail(context.Context, Claim, Failure) error
	RecoverExpired(context.Context, int) (int, error)
}

var _ Queue = (*PostgresStore)(nil)
