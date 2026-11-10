# soup-job-queue

A training-job queue backed by PostgreSQL.

## Current implementation

The service, four-worker deployment, chaos harness, load driver, independent audit, and delivery documentation are implemented. Execution is simulated with a cancellable timer and a database effect row. Full database/chaos/load verification remains pending; see the [evidence index](docs/evidence.md) and [DON'T SHIP verdict](docs/verdict.md).

## Start with Docker Compose

Requires Docker Engine/Desktop with Compose v2. For WSL, enable Docker Desktop integration for this distribution.

```sh
docker compose up --build
curl --fail http://localhost:8080/healthz
curl --fail http://localhost:8080/readyz
```

Compose waits for PostgreSQL, applies migrations, then starts the API and four workers. The database uses a named volume; `docker compose down` preserves data. `docker compose down -v` **deletes** that data. The included database credentials are for local development.

| Endpoint | Response |
| --- | --- |
| `GET /healthz` | 200, `{"status":"ok"}`; independent of the database |
| `GET /readyz` | 200, `{"status":"ready"}` if the expected migrations are applied; otherwise 503, `{"status":"not_ready"}` |

## Run locally

Requires Go 1.26+ and PostgreSQL 18. Create a database owned by your development user, then:

```sh
export DATABASE_URL='postgres://soup:soup@localhost:5432/soup?sslmode=disable'
go run ./cmd/migrate
go run ./cmd/api
```

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | Required | PostgreSQL connection string; never commit real credentials |
| `HTTP_ADDR` | `:8080` | HTTP bind address |
| `DB_TIMEOUT` | `3s` | Connection and readiness query timeout |
| `DB_MAX_CONNS` | `10` | Maximum database connections per process |
| `SHUTDOWN_TIMEOUT` | `10s` | Time allowed for API requests to finish on shutdown |

The API starts even if the database is unavailable; readiness reports that condition. Migrations are an explicit command and do not run inside the API process. Migration commands have a one-minute deadline, serialize using a transaction advisory lock, and reject changed or unknown applied migrations. Add a new numbered SQL file instead of editing an applied one.

## Verify

```sh
make build
make test
go test -race ./...
make vet
TEST_DATABASE_URL='postgres://soup:soup@localhost:5432/soup_test?sslmode=disable' make test-integration
```

Integration tests require a **disposable database** whose user can create schemas. Each test creates and drops its own schema. They cover readiness before/after migrations, migration reruns, checksums, constraints, indexes, concurrent idempotent creation, equal-timestamp pagination, cancellation, and replay after a lost response. `go test ./...` skips integration tests if `TEST_DATABASE_URL` is absent; the explicit Make target refuses to run without it.

To check container persistence, insert a temporary job, run `docker compose down`, start again, and query it:

```sh
docker compose exec db psql -U soup -d soup -c "INSERT INTO jobs (idempotency_key, request) VALUES ('persistence-check', jsonb_build_object('duration_ms',100,'simulate_failure',false)) ON CONFLICT DO NOTHING"
docker compose down
docker compose up --build -d
docker compose exec db psql -U soup -d soup -c "SELECT id, status FROM jobs WHERE idempotency_key = 'persistence-check'"
```

Wait for the database to become healthy before the final query. The expected result is the original row; its status may be succeeded because workers process it.

Verification performed in the development sandbox is recorded in [docs/task-1-verification.md](docs/task-1-verification.md). AI assistance is recorded in [docs/ai-usage.md](docs/ai-usage.md).

Implementation references: [PostgreSQL constraints](https://www.postgresql.org/docs/18/ddl-constraints.html), [pgx connection pools](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool).

## Job API

| Endpoint | Result |
| --- | --- |
| `POST /jobs` | 201 for a new job; 200 for a matching retry; 409 if the same key has different input |
| `GET /jobs/{id}` | Job details, or 404 |
| `GET /jobs?limit=50&status=queued&cursor=...` | `jobs` array and `next_cursor` (null on the final page) |
| `POST /jobs/{id}/cancel` | 200 for queued/running/already-cancelled jobs; 409 for succeeded/failed jobs; 404 if absent |

Create a job:

```sh
curl -i http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-training-1' \
  -d '{"duration_ms":100,"simulate_failure":false}'
```

Repeat that request with the same key to recover the same job. `duration_ms` is required and must be an integer from 1 to 5000. `simulate_failure` defaults to false; omitted and explicit false are equivalent on retries. Keys must contain 1–128 bytes and cannot be entirely whitespace. Input bodies are limited to 16 KiB; unknown fields and multiple JSON values are rejected.

Use the returned UUID in these requests:

```sh
curl http://localhost:8080/jobs/JOB_UUID
curl 'http://localhost:8080/jobs?limit=10&status=queued'
curl -X POST http://localhost:8080/jobs/JOB_UUID/cancel
```

Pass `next_cursor` unchanged as the next request's `cursor`, with the same status filter. Omit `status` to list all states. Limits range from 1 to 100. Jobs are ordered by creation timestamp and UUID. Pagination traverses live data; concurrent status changes can affect filtered results between requests. Cursors are opaque navigation values, not security credentials.

Errors use `{"error":{"code":"...","message":"..."}}`. Invalid input returns 400, oversized input 413, unsupported content type 415, and a request/database deadline 503. A response may be lost after creation commits: retry with the original idempotency key. Cancellation invalidates the existing claim and clears lease ownership; the worker persistence operations check that ownership before renewal or completion. Workers detect lost ownership on heartbeat or finalization and abandon that attempt.

Task 2 verification and pending database checks: [docs/task-2-verification.md](docs/task-2-verification.md).


## Worker persistence operations (Task 3)

`PostgresStore` now implements `jobs.Queue` for the worker runtime:

| Method | Behavior |
| --- | --- |
| `ClaimNext(ctx, workerID)` | Atomically claims the oldest unlocked queued job; returns nil when no work is available |
| `Renew(ctx, claim)` | Extends a current, unexpired claim |
| `Finish(ctx, claim, result)` | Inserts one effect and commits success in the same transaction |
| `Fail(ctx, claim, reason)` | Records a terminal execution failure with no effect |
| `RecoverExpired(ctx, limit)` | Requeues expired jobs or fails them at the attempt limit; handles 1–1000 rows per call |

The default lease is 10 seconds and jobs allow five attempts. `NewStoreWithLease` accepts a custom positive lease from one microsecond through 24 hours for worker configuration. Renewal does not update the caller's job snapshot; the database remains authoritative.

Claims use `FOR UPDATE SKIP LOCKED`, giving approximate FCFS claim order while allowing workers to proceed past locked jobs. A claim version fences stale attempts, including when a restarted worker reuses the same ID. Renewal/completion/failure lock the job row, check ownership, then check actual PostgreSQL time after any lock wait. Once validated, the lock reserves the transition until commit or rollback; keep these transactions short. Recovery and cancellation acquire that same row lock.

Repeated completion returns `ErrOwnershipLost`; it cannot create another effect. An unexpected pre-existing effect returns `ErrInvariantViolation`. `ErrCommitUnknown` means a disconnected commit may have reached PostgreSQL: use a fresh connection/context to read the job and compare its persisted status and claim version before retrying. Do not report success solely from that error or repeat an external side effect. The exactly-once effect guarantee is scoped to the simulated database row.

To exercise concurrency against a disposable PostgreSQL database:

```sh
TEST_DATABASE_URL='postgres://soup:soup@localhost:5432/soup_test?sslmode=disable' \
  go test -race -count=10 ./internal/jobs
```

Test coverage and environment limits: [docs/task-3-verification.md](docs/task-3-verification.md). Design references: [PostgreSQL row locking](https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE), [database clock functions](https://www.postgresql.org/docs/18/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT).


## Run workers (Task 4)

```sh
# Compose starts all four workers automatically.
docker compose up --build
# Inspect lifecycle logs.
docker compose logs -f worker-1 worker-2 worker-3 worker-4
# Or run an additional worker locally against DATABASE_URL after migrations.
WORKER_ID=local-worker go run ./cmd/worker
```

Each process handles one job at a time. A separate recovery loop scans expired leases even while the process is executing a job. Empty-queue polling adds jitter; failed claims use backoff capped at two seconds before jitter. An execution with `simulate_failure=true` records `execution_failed` with no effect.

| Worker setting | Default | Purpose |
| --- | --- | --- |
| `WORKER_ID` | Random per process | Identity included in claims and logs; Compose sets worker-1 through worker-4 |
| `LEASE_DURATION` | `10s` | Lease assigned and renewed by the database |
| `HEARTBEAT_INTERVAL` | `2s` | Renewal interval while executing |
| `POLL_INTERVAL` | `100ms` | Idle polling interval before jitter |
| `RECOVERY_INTERVAL` | `1s` | Interval between expired-job scans |
| `RECOVERY_BATCH` | `100` | Maximum jobs recovered per scan (1–1000) |

`DB_TIMEOUT` also bounds worker database operations. Heartbeat interval plus database timeout must be less than the lease duration. Runtime leases must be between 1ms and 24h, and poll/recovery intervals must be positive and no more than one minute. Compose gives each worker four database connections, allowing claim/heartbeat and recovery operations to proceed independently.

SIGINT/SIGTERM cancels execution, stops heartbeats and recovery, and waits for those operations to return. An unfinished job remains running until its lease expires and a surviving/restarted worker recovers it. Any heartbeat error abandons the attempt; infrastructure errors do not turn into execution failures. Unexpected duplicate effects stop the worker with an error. Unknown completion commits are checked with a fresh bounded read and are never blindly repeated.

Compose workers restart after unexpected exits. At startup the worker verifies migrations; startup database failures cause an exit and Compose retries. During normal operation, transient claim and recovery failures are logged and retried. Structured logs include worker ID and, for active attempts, job ID, claim version, and attempt count.

Worker unit tests are part of `make test`. `make test-integration` also includes worker lifecycle and SIGKILL/replacement tests on a disposable PostgreSQL database. Environment limitations and exact checks: [docs/task-4-verification.md](docs/task-4-verification.md).

References: [Go signal contexts](https://pkg.go.dev/os/signal#NotifyContext), [Compose configuration fragments](https://docs.docker.com/reference/compose-file/fragments/).


## Chaos harness and load evaluation (Tasks 5–6)

Run these commands from the repository root with an accessible PostgreSQL database. The experiment user must be able to create schemas. Each invocation creates a fresh schema, applies migrations, starts the real API handler on an ephemeral local HTTP port, and supervises its own worker processes. It never clears the normal application's tables. Failed and successful experiment schemas remain available for diagnosis; remove a recorded experiment schema manually only after preserving the evidence you need.

```sh
export DATABASE_URL='postgres://soup:soup@localhost:5432/soup?sslmode=disable'
make build
TEST_DATABASE_URL='postgres://soup:soup@localhost:5432/soup_test?sslmode=disable' make test-integration
./bin/chaos -seed 1
./bin/chaos -seed 2
./bin/chaos -seed 3
./bin/load -mode mixed
./bin/load -mode create
```

`chaos` defaults to 10,000 unique jobs, eight workers, a random SIGKILL roughly every two seconds, and at least 30 seconds of parallel retry/cancellation/fault traffic (longer if initial creation takes longer). Durations are seeded 20–100 ms with approximately 2% simulated execution failures. After fault traffic stops, the harness restores the configured worker count and allows up to five minutes to drain. It requires repeated POSTs, acknowledged cancellations, and enough successful kills to count as coverage. Unexpected worker exits fail the run immediately; planned kills are replaced. Overall timeout is 15 minutes.

`load` defaults to eight workers, a 30-second warm-up, and five minutes of scheduled 100 RPS. The mixed profile uses 50% create, 30% read, 10% list, and 10% cancel; 10% of create requests replay their previous key. The create profile sends 100 unique job creations per second. New load jobs simulate 50–150 ms execution; 32 initial one-millisecond jobs supply IDs for reads/cancellations. Warm-up requests are excluded from performance statistics but retained in raw evidence.

The driver does not wait for a response before scheduling the next arrival. Requests beyond `-max-inflight` (default 256) are recorded as drops, not silently delayed. Raw samples contain scheduled, dispatched, and completed timestamps. Reports include response p50/p95/p99, scheduled-to-completion p95, scheduling-lag p95, dispatch/successful request RPS, execution completion rate, errors/drops, and queue growth. Raw metrics include queue age/states/attempts and API pool pressure. API and generator share a process, so their Go memory statistics are combined; worker CPU/max-RSS is recorded at exit and DB resource monitoring still requires external collection.

Experiment results are stored under a new `results/<kind>-<timestamp>-seed<seed>-<unique>/` directory every time. Each contains source/config/environment metadata, a manifest, raw JSONL events, and a final summary if the invocation reaches cleanup. Runs that start also have raw API/worker logs, per-phase HTTP counts, audit output, and (for load) a load report. No directory is overwritten. A run without a final summary is incomplete and should be kept. The CLI exits nonzero on failed preflight, workload errors, drain timeout, invariant violations, insufficient coverage, or unmet load targets.

An independent final audit compares every manifest key and normalized payload, checks terminal states, attempt limits, ownership cleanup, acknowledged cancellations, orphan/duplicate effects, and effect claim versions. Timed-out creations are retried with their original keys; load reconciliation happens outside the measured stream. That does not erase a measurement error/drop. See [silent failures](docs/silent-failures.md) for protections, targeted tests, and blind spots.

To run the same experiments inside Docker with eight child worker processes and persistent local evidence:

```sh
mkdir -p results
export EXPERIMENT_UID="$(id -u)" EXPERIMENT_GID="$(id -g)"
docker compose --profile experiments build chaos load
docker compose --profile experiments run --rm chaos -seed 1
docker compose --profile experiments run --rm chaos -seed 2
docker compose --profile experiments run --rm chaos -seed 3
docker compose --profile experiments run --rm load -mode mixed
docker compose --profile experiments run --rm load -mode create
```

The experiment services are opt-in and use an init process for child reaping; no Docker socket mount is needed. Their workers only read their fresh schema, so normal workers can coexist, although running the normal stack at the same time will compete for host/database resources. For capacity evaluation, use a dedicated test deployment and record its hardware limits.

Deliverables: [one-page design note](docs/design.md), [failure analysis](docs/silent-failures.md), [raw evidence index](docs/evidence.md), [verdict](docs/verdict.md), [reflection draft](docs/reflection.md), and [AI usage](docs/ai-usage.md). Verification details: [Tasks 5–6](docs/task-5-6-verification.md).
