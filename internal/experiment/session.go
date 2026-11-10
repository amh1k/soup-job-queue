package experiment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/amh1k/soup-job-queue/internal/audit"
	"github.com/amh1k/soup-job-queue/internal/httpapi"
	"github.com/amh1k/soup-job-queue/internal/jobs"
	"github.com/amh1k/soup-job-queue/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type sample struct {
	PoolAcquireNS                int64
	Succeeded, Failed, Cancelled int
	APIGoHeapBytes               uint64
	APIGoroutines                int
	Time                         time.Time
	Queued, Running              int
	OldestQueuedSeconds          float64
	Attempts                     int
	PoolWaitNS                   int64
	PoolEmptyAcquires            int64
	PoolConnections              int32
}
type session struct {
	pool           *pgxpool.Pool
	apiPool        *pgxpool.Pool
	base           string
	supervisor     *supervisor
	stopWorkers    context.CancelFunc
	workersDone    chan error
	server         *http.Server
	serverDone     chan error
	apiLog         *os.File
	previousLogger *slog.Logger
	monitorCancel  context.CancelFunc
	monitorDone    chan struct{}
	mu             sync.Mutex
	samples        []sample
	evidence       *evidence
	schema         string
	cancel         context.CancelCauseFunc
}

func openSession(ctx context.Context, cancel context.CancelCauseFunc, o Options, e *evidence) (s *session, err error) {
	raw := os.Getenv("DATABASE_URL")
	u, parseErr := url.Parse(raw)
	if parseErr != nil || u == nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return nil, errors.New("DATABASE_URL must be a PostgreSQL URL")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil {
		return nil, errors.New("invalid DATABASE_URL")
	}
	cfg.MaxConns = 4
	cfg.ConnConfig.ConnectTimeout = 3 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("cannot configure database pool")
	}
	s = &session{pool: pool, evidence: e, cancel: cancel}
	defer func(current *session) {
		if err != nil {
			current.close()
		}
	}(s)
	call, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if err = pool.Ping(call); err != nil {
		return nil, fmt.Errorf("database preflight: %w", err)
	}
	var version, isolation, zone, fsync, synchronous string
	if err = pool.QueryRow(call, `SELECT current_setting('server_version'),current_setting('transaction_isolation'),current_setting('TimeZone'),current_setting('fsync'),current_setting('synchronous_commit')`).Scan(&version, &isolation, &zone, &fsync, &synchronous); err != nil {
		return nil, err
	}
	e.event("database_settings", map[string]string{"version": version, "isolation": isolation, "timezone": zone, "fsync": fsync, "synchronous_commit": synchronous})
	s.schema = "experiment_" + fmt.Sprintf("%d", time.Now().UnixNano())
	if _, err = pool.Exec(call, "CREATE SCHEMA "+pgx.Identifier{s.schema}.Sanitize()); err != nil {
		return nil, err
	}
	// Retain this fresh schema for diagnosis, including failed/incomplete runs.
	e.event("schema_created", s.schema)
	q := u.Query()
	q.Set("search_path", s.schema)
	u.RawQuery = q.Encode()
	isolated, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, errors.New("cannot configure isolated schema")
	}
	isolated.MaxConns = 4
	isolated.ConnConfig.ConnectTimeout = 3 * time.Second
	isolatedPool, err := pgxpool.NewWithConfig(ctx, isolated)
	if err != nil {
		return nil, err
	}
	pool.Close()
	s.pool = isolatedPool
	if err = migrations.Apply(call, s.pool); err != nil {
		return nil, err
	}
	apiCfg := isolated.Copy()
	apiCfg.MaxConns = 10
	s.apiPool, err = pgxpool.NewWithConfig(ctx, apiCfg)
	if err != nil {
		return nil, err
	}
	s.apiLog, err = os.OpenFile(filepath.Join(e.dir, "api.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	logger := slog.New(slog.NewJSONHandler(s.apiLog, nil))
	s.previousLogger = slog.Default()
	slog.SetDefault(logger)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s.base = "http://" + listener.Addr().String()
	handler := httpapi.NewAPIHandler(jobs.NewStore(s.apiPool), func(ctx context.Context) error { return migrations.Check(ctx, s.apiPool) }, 3*time.Second)
	s.server = &http.Server{ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelError), Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Info("request", "method", r.Method, "path", r.URL.Path)
		handler.ServeHTTP(w, r)
	}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: time.Minute}
	s.serverDone = make(chan error, 1)
	go func() {
		serveErr := s.server.Serve(listener)
		if !errors.Is(serveErr, http.ErrServerClosed) {
			cancel(serveErr)
		}
		s.serverDone <- serveErr
	}()
	workersCtx, stopWorkers := context.WithCancel(ctx)
	s.stopWorkers = stopWorkers
	s.workersDone = make(chan error, 1)
	s.supervisor = newSupervisor(o.WorkerBinary, u.String(), o.Workers, e)
	ready := make(chan error, 1)
	go func() {
		runErr := s.supervisor.run(workersCtx, ready)
		if runErr != nil {
			cancel(runErr)
		}
		s.workersDone <- runErr
	}()
	if err = <-ready; err != nil {
		return nil, err
	}
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	s.monitorCancel = stopMonitor
	s.monitorDone = make(chan struct{})
	go s.monitor(monitorCtx)
	e.event("session_started", map[string]any{"schema": s.schema, "api": s.base, "settings": s.supervisor.settings()})
	return s, nil
}
func (s *session) quiesce() {
	if s.monitorCancel != nil {
		s.monitorCancel()
		<-s.monitorDone
		s.monitorCancel = nil
	}
	if s.stopWorkers != nil {
		s.stopWorkers()
		if err := <-s.workersDone; err != nil {
			s.evidence.event("supervisor_error", err.Error())
		}
		s.stopWorkers = nil
	}
}
func (s *session) close() {
	s.quiesce()
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.server.Shutdown(ctx)
		cancel()
		_ = s.server.Close()
		<-s.serverDone
		s.server = nil
	}
	if s.previousLogger != nil {
		slog.SetDefault(s.previousLogger)
	}
	if s.apiLog != nil {
		_ = s.apiLog.Close()
	}
	if s.apiPool != nil {
		s.apiPool.Close()
	}
	if s.pool != nil {
		s.pool.Close()
	}
}
func (s *session) monitor(ctx context.Context) {
	defer close(s.monitorDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, 3*time.Second)
		var row sample
		row.Time = time.Now().UTC()
		err := s.pool.QueryRow(call, `SELECT count(*) FILTER(WHERE status='queued'),count(*) FILTER(WHERE status='running'),count(*) FILTER(WHERE status='succeeded'),count(*) FILTER(WHERE status='failed'),count(*) FILTER(WHERE status='cancelled'),coalesce(extract(epoch FROM clock_timestamp()-min(created_at) FILTER(WHERE status='queued')),0)::float8,coalesce(sum(attempt_count),0) FROM jobs`).Scan(&row.Queued, &row.Running, &row.Succeeded, &row.Failed, &row.Cancelled, &row.OldestQueuedSeconds, &row.Attempts)
		cancel()
		if err != nil {
			s.evidence.event("monitor_error", err.Error())
		} else {
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			row.APIGoHeapBytes = memory.HeapAlloc
			row.APIGoroutines = runtime.NumGoroutine()
			stats := s.apiPool.Stat()
			row.PoolAcquireNS = stats.AcquireDuration().Nanoseconds()
			row.PoolWaitNS = stats.EmptyAcquireWaitTime().Nanoseconds()
			row.PoolEmptyAcquires = stats.EmptyAcquireCount()
			row.PoolConnections = stats.TotalConns()
			s.mu.Lock()
			s.samples = append(s.samples, row)
			s.mu.Unlock()
			s.evidence.event("metrics", row)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (s *session) drain(ctx context.Context, timeout time.Duration) error {
	drain, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for drain.Err() == nil {
		call, stop := context.WithTimeout(drain, 3*time.Second)
		var count int
		err := s.pool.QueryRow(call, `SELECT count(*) FROM jobs WHERE status IN ('queued','running')`).Scan(&count)
		stop()
		if err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		if !sleep(drain, 200*time.Millisecond) {
			break
		}
	}
	return fmt.Errorf("drain deadline: %w", drain.Err())
}
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(max(d, 0))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (s *session) audit(ctx context.Context, entries []audit.Entry, cancelled map[string]bool) (audit.Report, error) {
	s.quiesce()
	call, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	report, err := audit.Inspect(call, s.pool, entries, cancelled)
	if err == nil {
		err = s.evidence.json("audit.json", report)
	}
	return report, err
}
