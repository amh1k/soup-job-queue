package experiment

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
	"time"

	"github.com/amh1k/soup-job-queue/internal/audit"
	"github.com/amh1k/soup-job-queue/internal/jobs"
)

type Summary struct {
	Kind                                              string `json:"kind"`
	Started, Ended                                    time.Time
	Success                                           bool    `json:"success"`
	Error                                             string  `json:"error,omitempty"`
	WorkloadStarted                                   bool    `json:"workload_started"`
	Settings                                          Options `json:"settings"`
	Schema                                            string  `json:"schema,omitempty"`
	Kills, UnexpectedWorkerExits                      int64
	ExpectedJobs, AcceptedJobs, AcceptedCancellations int
	Audit                                             *audit.Report `json:"audit,omitempty"`
	Load                                              *LoadReport   `json:"load,omitempty"`
}

func Run(ctx context.Context, kind string, o Options) (runErr error) {
	e, err := newEvidence(o.Output, kind, o.Seed)
	if err != nil {
		return err
	}
	summary := Summary{Kind: kind, Started: time.Now().UTC(), Settings: o}
	defer func() {
		if recovered := recover(); recovered != nil {
			runErr = fmt.Errorf("harness panic: %v", recovered)
			e.event("panic", map[string]string{"message": fmt.Sprint(recovered), "stack": string(debug.Stack())})
		}
		e.mu.Lock()
		eventErr := e.err
		e.mu.Unlock()
		runErr = errors.Join(runErr, eventErr)
		summary.Ended = time.Now().UTC()
		summary.Success = runErr == nil
		if runErr != nil {
			summary.Error = runErr.Error()
		}
		e.event("summary", summary)
		if err := e.json("summary.json", summary); runErr == nil && err != nil {
			runErr = err
		}
		if err := e.close(); runErr == nil && err != nil {
			runErr = err
		}
		fmt.Printf("%s success=%v error=%s\n", e.dir, summary.Success, summary.Error)
	}()
	if kind != "chaos" && kind != "load" {
		return errors.New("unknown workload kind")
	}
	if err := o.validate(kind); err != nil {
		return err
	}
	ctx, timeout := context.WithTimeout(ctx, o.Timeout)
	defer timeout()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var entries []audit.Entry
	var operations []operation
	if kind == "chaos" {
		entries = manifest(o.Jobs, o.Seed, 20, 100, true)
	} else {
		entries, operations = loadPlan(o)
	}
	summary.ExpectedJobs = len(entries)
	if err := e.json("manifest.json", entries); err != nil {
		return err
	}
	if kind == "load" {
		if err := e.json("operations.json", operations); err != nil {
			return err
		}
	}
	session, err := openSession(ctx, cancel, o, e)
	if err != nil {
		return err
	}
	defer session.close()
	summary.Schema = session.schema
	c := newClient(session.base, e)
	defer c.http.CloseIdleConnections()
	defer func() {
		c.mu.Lock()
		summary.AcceptedJobs = len(c.ids)
		summary.AcceptedCancellations = len(c.cancelled)
		c.mu.Unlock()
		runErr = errors.Join(runErr, e.json("http-counts.json", c.counts))
		summary.Kills = session.supervisor.Kills.Load()
		summary.UnexpectedWorkerExits = session.supervisor.Unexpected.Load()
	}()
	summary.WorkloadStarted = true
	if kind == "chaos" {
		err = runChaos(ctx, o, session, c, entries)
	} else {
		summary.Load, err = runLoad(ctx, o, session, c, entries, operations)
	}
	// Even a workload/drain failure gets a final diagnostic audit, with a fresh
	// bounded context if the main deadline has elapsed. It must never become PASS.
	if ctx.Err() == nil {
		err = errors.Join(err, session.drain(ctx, o.DrainTimeout))
	}
	auditCtx, stopAudit := context.WithTimeout(context.Background(), 15*time.Second)
	defer stopAudit()
	report, auditErr := session.audit(auditCtx, entries, c.cancellations())
	if auditErr == nil {
		summary.Audit = &report
		if !report.Valid {
			err = errors.Join(err, fmt.Errorf("audit failed: %d violations", len(report.Violations)))
		}
	} else {
		err = errors.Join(err, auditErr)
	}
	if session.supervisor.Unexpected.Load() != 0 {
		err = errors.Join(err, errors.New("unexpected worker exits occurred"))
	}
	if kind == "chaos" && session.supervisor.Kills.Load() == 0 {
		err = errors.Join(err, errors.New("no worker was killed; chaos coverage incomplete"))
	}
	if ctx.Err() != nil {
		err = errors.Join(err, context.Cause(ctx))
	}
	return err
}
func manifest(n int, seed int64, minMS, maxMS int, failures bool) []audit.Entry {
	random := rand.New(rand.NewSource(seed))
	result := make([]audit.Entry, n)
	for i := range result {
		result[i] = audit.Entry{Key: fmt.Sprintf("job-%d-%08d", seed, i), Input: jobs.CreateInput{DurationMS: minMS + random.Intn(maxMS-minMS+1), SimulateFailure: failures && random.Intn(50) == 0}}
	}
	return result
}
