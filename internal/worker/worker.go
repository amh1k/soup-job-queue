package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/amh1k/soup-job-queue/internal/jobs"
)

// Store includes Get so an uncertain commit can be reconciled without executing
// the job's effect again. Each Worker handles at most one active claim.
type Store interface {
	jobs.Queue
	Get(context.Context, string) (jobs.Job, error)
}
type Options struct {
	ID                string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
	RecoveryInterval  time.Duration
	DBTimeout         time.Duration
	RecoveryBatch     int
}

func (o Options) Validate() error {
	if strings.TrimSpace(o.ID) == "" || len(o.ID) > 128 {
		return errors.New("WORKER_ID must contain 1–128 bytes")
	}
	if o.LeaseDuration < time.Millisecond || o.LeaseDuration > 24*time.Hour {
		return errors.New("LEASE_DURATION must be between 1ms and 24h")
	}
	if o.HeartbeatInterval <= 0 || o.DBTimeout <= 0 || o.HeartbeatInterval >= o.LeaseDuration || o.DBTimeout >= o.LeaseDuration-o.HeartbeatInterval {
		return errors.New("HEARTBEAT_INTERVAL plus DB_TIMEOUT must be less than LEASE_DURATION")
	}
	if o.PollInterval <= 0 || o.PollInterval > time.Minute || o.RecoveryInterval <= 0 || o.RecoveryInterval > time.Minute {
		return errors.New("poll and recovery intervals must be positive and at most 1m")
	}
	if o.RecoveryBatch < 1 || o.RecoveryBatch > 1000 {
		return errors.New("RECOVERY_BATCH must be between 1 and 1000")
	}
	return nil
}

type Worker struct {
	store   Store
	options Options
	log     *slog.Logger
}

func New(store Store, options Options, logger *slog.Logger) (*Worker, error) {
	if store == nil {
		return nil, errors.New("worker store is required")
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{store: store, options: options, log: logger.With("worker_id", options.ID)}, nil
}
func (w *Worker) Run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); w.recover(ctx) }()
	defer func() { cancel(); <-done }()
	w.log.Info("worker started")
	defer w.log.Info("worker stopped")
	backoff := min(w.options.PollInterval, 2*time.Second)
	for ctx.Err() == nil {
		call, stop := context.WithTimeout(ctx, w.options.DBTimeout)
		claim, err := w.store.ClaimNext(call, w.options.ID)
		stop()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			w.log.Warn("claim failed; retrying", "error", err)
			if !wait(ctx, jitter(backoff)) {
				return nil
			}
			backoff = min(backoff*2, 2*time.Second)
			continue
		}
		backoff = min(w.options.PollInterval, 2*time.Second)
		if claim == nil {
			if !wait(ctx, jitter(w.options.PollInterval)) {
				return nil
			}
			continue
		}
		if err := w.execute(ctx, *claim); err != nil {
			return err
		}
	}
	return nil
}
func (w *Worker) execute(ctx context.Context, claim jobs.Claim) error {
	log := w.log.With("job_id", claim.Job.ID, "claim_version", claim.Version, "attempt", claim.Job.AttemptCount)
	log.Info("job claimed")
	// A timer emulates cancellable execution; only Finish writes the actual effect.
	execution := time.NewTimer(time.Duration(claim.Job.Request.DurationMS) * time.Millisecond)
	defer execution.Stop()
	heartbeat := time.NewTicker(w.options.HeartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("attempt abandoned on shutdown")
			return nil
		case <-heartbeat.C:
			call, cancel := context.WithTimeout(ctx, w.options.DBTimeout)
			err := w.store.Renew(call, claim)
			cancel()
			if err != nil {
				if errors.Is(err, jobs.ErrOwnershipLost) {
					log.Info("claim ownership lost")
				} else {
					log.Warn("heartbeat failed; abandoning attempt", "error", err)
				}
				return nil
			}
		case <-execution.C:
			if ctx.Err() != nil {
				return nil
			}
			call, cancel := context.WithTimeout(ctx, w.options.DBTimeout)
			target := jobs.Succeeded
			var err error
			if claim.Job.Request.SimulateFailure {
				target = jobs.Failed
				err = w.store.Fail(call, claim, jobs.Failure{Code: "execution_failed", Message: "Simulated execution failure"})
			} else {
				err = w.store.Finish(call, claim, jobs.Result{Message: "Simulated training completed"})
			}
			cancel()
			if err == nil {
				log.Info("job completed", "status", target)
				return nil
			}
			if errors.Is(err, jobs.ErrInvariantViolation) {
				return fmt.Errorf("job %s: %w", claim.Job.ID, err)
			}
			if errors.Is(err, jobs.ErrOwnershipLost) {
				log.Info("completion rejected; claim ownership lost")
				return nil
			}
			if errors.Is(err, jobs.ErrCommitUnknown) {
				// The timed-out operation context cannot be reused. A fresh bounded read
				// inspects durable state; it never repeats the effect or terminal write.
				readCtx, stop := context.WithTimeout(ctx, w.options.DBTimeout)
				job, readErr := w.store.Get(readCtx, claim.Job.ID)
				stop()
				if readErr == nil && job.Status == target && job.ClaimVersion == claim.Version {
					log.Info("completion reconciled", "status", target)
					return nil
				}
				log.Warn("completion outcome unresolved; lease recovery will reconcile", "error", err, "read_error", readErr)
			} else {
				log.Warn("completion failed; abandoning attempt", "error", err)
			}
			return nil
		}
	}
}
func (w *Worker) recover(ctx context.Context) {
	ticker := time.NewTicker(w.options.RecoveryInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, w.options.DBTimeout)
		n, err := w.store.RecoverExpired(call, w.options.RecoveryBatch)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			w.log.Warn("recovery scan failed", "error", err)
		} else if n > 0 {
			w.log.Info("expired jobs recovered", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func jitter(d time.Duration) time.Duration { return d + time.Duration(rand.Int64N(int64(d/4)+1)) }
