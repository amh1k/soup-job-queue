# soup-job-queue

A durable training-job queue built with Go and PostgreSQL. Clients submit and cancel jobs through an HTTP API; independent worker processes claim and execute them concurrently.

## Implementation and verification status

The API, worker runtime, four-worker Compose deployment, chaos harness, load driver, independent audit, and delivery documents are implemented. Training is simulated with a timer; the committed result is a row in `job_effects`.

**Measured results:** three 10,000-job chaos runs with eight workers passed their final audits, with 14 planned worker kills each. The 100 RPS mixed load passed. At 100 unique creates/second, eight workers completed about 70 jobs/second and the queue grew, so the create-only profile failed capacity acceptance; all jobs eventually completed and its final audit passed. The current verdict remains **DON'T SHIP at an unrestricted 100 RPS**, especially with four default deployment workers. The full Go race-test suite passed against PostgreSQL with zero skips, including long-running heartbeat and paused-worker recovery tests; Compose readiness and restart/persistence checks also passed. See the [evidence index](docs/evidence.md) and [verdict](docs/verdict.md).

## Assignment deliverables

| Requested deliverable | Location |
| --- | --- |
| Runnable service and usage instructions | This README; [`compose.yaml`](compose.yaml) |
| One-page design note | [Design](docs/design.md) |
| Chaos harness, run counts, raw output including failures, and blind spots | [`cmd/chaos`](cmd/chaos), [evidence index](docs/evidence.md), [raw archives](results/archives) |
| Silent-failure analysis and checks | [Silent failures](docs/silent-failures.md) |
| SHIP / DON'T SHIP at 100 RPS and fixes | [Verdict](docs/verdict.md) |
| Reflection paragraph | [Reflection draft](docs/reflection.md) |
| AI usage disclosure | [AI usage](docs/ai-usage.md) |

## Reading guide

- [Architecture](#architecture): API, workers, PostgreSQL, and process boundaries.
- [Job lifecycle](#job-lifecycle): submission, claims, execution, and completion.
- [Leases and crash recovery](#leases-and-crash-recovery): heartbeats and stale attempts.
- [Transactions and cancellation](#transactions-and-cancellation): consistent state and effects.
- [Inside the chaos harness](#inside-the-chaos-harness): goroutines, processes, signals, and audit.
- [Code map](#code-map): where each responsibility lives.
- [Start with Docker Compose](#start-with-docker-compose), [Job API](#job-api), and [Run workers](#run-workers).
- [Verify](#verify) and [Chaos harness and load evaluation](#chaos-harness-and-load-evaluation).

## Architecture

The public API manages **jobs**. Workers are execution processes started by Compose or the experiment supervisor; there is no worker CRUD API. A client does not assign a job to a particular worker. Available workers compete to claim queued jobs from PostgreSQL.

```mermaid
flowchart LR
    Client["Client / HTTP caller"] -->|"create, get, list, cancel"| API["API process"]
    API --> Store["PostgresStore"]
    Store --> DB[("PostgreSQL: jobs + job_effects")]
    subgraph Workers["Independent worker processes"]
        W1["Worker 1"]
        W2["Worker 2"]
        WN["Worker 3 ... N"]
    end
    W1 <-->|"claim, renew, recover, finish"| DB
    W2 <-->|"claim, renew, recover, finish"| DB
    WN <-->|"claim, renew, recover, finish"| DB
    Migrate["Migration program: runs then exits"] --> DB
```

Normal Compose startup launches PostgreSQL, waits for it to become healthy, runs migrations, then starts the API and four workers. The experiment harness starts eight workers by default in a separate schema.

### Where is the queue? Why PostgreSQL instead of Redis?

The `jobs` table is both durable job storage and the queue. Rows with `status = 'queued'` are available work; workers poll for them. A partial index on `(created_at, id)` supports choosing queued work. There is no separate in-memory queue.

This keeps job creation, ownership, recovery, cancellation, and committed effects in one transactional system. Redis could provide a notification or delivery layer, but keeping Redis delivery consistent with PostgreSQL would require another protocol, such as a transactional outbox. PostgreSQL avoids that extra coordination for this scope.

Database polling still costs queries, connections, and row locks. Workers add jitter to idle polling and back off after claim errors. Capacity must be measured; this design is not evidence that the database can sustain any particular workload.

### Processes, goroutines, and communication

| Component | Runs as | Responsibility |
| --- | --- | --- |
| API | One process | Serve HTTP requests through `httpapi` and the store |
| Each worker | Separate process | Execute at most one active job; run a recovery goroutine alongside the claim/execution loop |
| Migrations | Separate short-lived process | Apply/check migration history, then exit |
| Chaos harness | One process | Host the experiment API, generate requests, supervise workers, collect evidence |
| Experiment supervisor | Goroutine inside the harness | Start workers, observe exits, replace planned kills |

Separate worker processes let the harness kill one worker with `SIGKILL` while the others continue. Go goroutines share their process: killing a process would stop every goroutine inside it. Workers coordinate through PostgreSQL. The API does not send assignments directly to worker processes.

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> queued: New idempotency key
    queued --> running: Atomic claim
    queued --> cancelled: Cancellation
    running --> succeeded: Effect and success commit together
    running --> failed: Simulated execution failure
    running --> cancelled: Cancellation wins the row lock
    running --> queued: Expired lease, attempts remain
    running --> failed: Expired lease, attempt limit reached
    succeeded --> [*]
    failed --> [*]
    cancelled --> [*]
```

1. The client sends `POST /jobs` with an `Idempotency-Key` and execution input.
2. The API validates the request and asks `PostgresStore` to create a queued job. A database uniqueness constraint prevents duplicate keys. A matching retry returns the same job; different input with that key returns 409.
3. A worker atomically selects and claims an available job, changing it to running and recording its owner, lease, attempt count, and claim version.
4. The claim transaction ends. The worker simulates execution with a timer and renews its lease while working. **No database transaction remains open during the simulated training.**
5. Successful execution inserts an effect and marks the job succeeded in one short transaction. Simulated execution failure records a failed job with no effect. Infrastructure errors abandon the attempt for lease recovery.
6. Clients retrieve status using the job API. Jobs are not pushed back to clients when they finish.

### Concurrency and approximate FCFS

Claims order queued jobs by `(created_at, id)` and use `FOR UPDATE SKIP LOCKED` inside an atomic update. If worker A locks a candidate, worker B skips it and can claim another job. The committed update makes the first job running, so subsequent claims no longer select it.

This provides **approximate first come, first served claim order**. A locked older job can be skipped; execution duration and retries also mean completion order differs from creation order. Recovery preserves the original creation timestamp. There are no configurable priority or scheduling policies.

## Leases and crash recovery

A lease is a deadline for the current worker's ownership. The default is **10 seconds**, renewed by a heartbeat every **2 seconds**. Each worker also scans for expired leases every **1 second**, so recovery continues while other workers execute jobs. All lease decisions use PostgreSQL time.

A lease does not prevent a crash. It makes abandoned work eligible for recovery without requiring the database to know whether the worker process is alive.

```mermaid
sequenceDiagram
    participant A as Worker A
    participant DB as PostgreSQL
    participant B as Surviving worker B
    A->>DB: Claim job; owner A, version 1, lease deadline
    DB-->>A: Claim committed
    Note over A: Process killed or stops responding
    Note over DB: Job remains running until lease expires
    B->>DB: Recovery scan locks expired job
    DB->>DB: Clear ownership; increment version; requeue
    B->>DB: Claim queued job with a newer version
    DB-->>B: New claim committed
    B->>DB: Finish with current owner and version
    DB-->>B: Effect and success committed
```

Recovery requeues expired jobs while attempts remain. At the default limit of five claims, an expired attempt becomes failed with `attempts_exhausted`. A replacement process may claim any eligible job; it does not automatically inherit the killed worker's job.

### Why also use a claim version?

The claim operation prevents two workers from newly claiming the same queued row. A **claim version** solves a later problem: an old execution attempt may still try to write after its ownership has expired. For example, A pauses, its lease expires, B recovers and claims the job, then A resumes with an old result.

A does not need to reclaim the job to cause trouble; it already holds a previous claim in memory. Renewal, completion, and failure therefore validate all of these under a row lock:

- The job is still running.
- The stored worker ID matches the caller.
- The stored claim version matches the caller's attempt.
- The lease is still valid according to the actual database clock after acquiring the lock.

Recovery and cancellation invalidate old claims by incrementing the version. Reusing a worker ID after restart cannot make an old claim current again. The version is a fencing number for a particular ownership period, not an authentication secret.

## Transactions and cancellation

### Why have a `job_effects` table?

Execution attempts and committed results are different facts. A worker may simulate the same job again after a crash, but a successful job must have exactly one committed effect. `job_effects` makes that effect countable and ties it to the claim version that committed it. Its primary key allows at most one effect per job.

```mermaid
flowchart TD
    Begin["BEGIN: short completion transaction"] --> Lock["Lock job row"]
    Lock --> Validate{"Current owner, version, running state, valid lease?"}
    Validate -->|No| Reject["Reject attempt and roll back"]
    Validate -->|Yes| Effect["Insert unique job_effects row"]
    Effect --> Success["Mark succeeded; clear owner and lease"]
    Success --> Commit["COMMIT both changes"]
```

If either write fails, the transaction rolls both back. A process dying before commit leaves neither change committed; dying after commit leaves both. The guarantee is **at-least-once execution attempts, with exactly one database effect for a successful job**. Failed and cancelled jobs have zero effects. External actions such as email, file uploads, or remote payments would need their own idempotency mechanism.

If the connection fails during commit, the worker cannot infer whether PostgreSQL committed. It makes a fresh bounded read and compares status and claim version; it never blindly repeats the effect.

### What if cancellation races with completion?

Cancellation and completion lock the same job row. If cancellation wins, it marks the job cancelled, clears ownership, and invalidates the claim. The worker's later completion is rejected. If completion wins, the committed job is succeeded and cancellation returns 409. Repeated cancellation of a cancelled job returns 200.

Cancellation is a database transition. There is no direct signal from the API to the worker process. The worker discovers lost ownership on its next heartbeat or finalization and abandons that attempt. Its simulated timer may briefly continue, but the old claim cannot commit an effect.

## Inside the chaos harness

The harness exercises the actual HTTP handler and PostgreSQL store. Its API server lives inside the harness process; its workers are separate child processes. This tests the job protocol, while Docker startup and deployment packaging need their own checks.

```mermaid
flowchart TB
    subgraph Harness["One chaos harness process"]
        Create["32 submission goroutines"]
        Retry["Duplicate submission goroutine"]
        Cancel["Cancellation goroutine"]
        Kill["Kill ticker goroutine: every 2s"]
        Supervisor["Supervisor goroutine"]
        Wait["cmd.Wait goroutines"]
        HTTP["Experiment HTTP API"]
        Audit["Independent final SQL audit"]
        Create --> HTTP
        Retry --> HTTP
        Cancel --> HTTP
        Kill -->|"call killRandom"| Supervisor
        Wait -->|"exit notifications: Go channel"| Supervisor
    end
    Supervisor -->|"start / SIGKILL through OS"| Children["8 separate worker processes"]
    Children -.->|"OS reports process exit"| Wait
    HTTP --> DB[("Fresh PostgreSQL schema per run")]
    Children <-->|"claims, heartbeats, recovery, results"| DB
    Audit -->|"read persisted facts"| DB
```

The kill ticker asks the supervisor to choose and kill a live worker. The supervisor records the planned kill and sends the OS signal. A goroutine waiting on `cmd.Wait()` receives the process exit status and sends it through an **in-process Go channel** to the supervisor. Workers do not send messages through that channel. Planned kills get replacements with new worker identities; unexpected exits fail the experiment.

The duplicate loop resends manifest keys with identical input and checks that each key returns the same job ID. The cancellation loop targets accepted jobs and records confirmed cancellations. These loops overlap the initial submissions and worker kills; the default fault period lasts at least 30 seconds and until initial submissions finish.

After fault injection stops, the harness restores its worker count, retries uncertain creations with their original keys, and lets the queue drain. It stops workers before auditing PostgreSQL directly, independently of the store methods being tested. The audit checks every expected key and payload, terminal states, ownership cleanup, attempt bounds, cancellation acknowledgements, and effect counts and versions. Missing fault coverage, timeouts, workload errors, or audit violations fail the run. Logs and summaries are retained for both successful and failed attempts.

The supervisor belongs to the experiments. During normal Compose operation, Docker's `restart: unless-stopped` policy restarts worker containers, and surviving workers recover expired jobs through PostgreSQL.

## Code map

| Path | What to read it for |
| --- | --- |
| [`cmd/api`](cmd/api) | API entry point: configuration, logging, database pool, HTTP server, shutdown |
| [`cmd/migrate`](cmd/migrate) | Independent migration program |
| [`cmd/worker`](cmd/worker) | Independent worker entry point |
| [`internal/httpapi`](internal/httpapi) | Routes, HTTP validation, JSON responses, status/error mapping |
| [`internal/jobs/model.go`](internal/jobs/model.go) | Job types and store/queue interfaces |
| [`internal/jobs/store.go`](internal/jobs/store.go) | PostgreSQL create/get/list/cancel implementation |
| [`internal/jobs/claims.go`](internal/jobs/claims.go) | Atomic claims, leases, fencing, completion transactions, recovery |
| [`internal/worker/worker.go`](internal/worker/worker.go) | Claim/execution loop, heartbeat handling, separate recovery loop |
| [`migrations`](migrations) | Tables, constraints, indexes, migration runner |
| [`internal/experiment/chaos.go`](internal/experiment/chaos.go) | Concurrent submissions, retries, cancellations, and kill timing |
| [`internal/experiment/supervisor.go`](internal/experiment/supervisor.go) | Child process lifecycle and exit channel |
| [`internal/experiment/session.go`](internal/experiment/session.go) | Isolated schema, experiment API, monitoring, drain, audit setup |
| [`internal/audit`](internal/audit) | Independent checks of persisted jobs and effects |
| [`cmd/load`](cmd/load), [`internal/experiment/load.go`](internal/experiment/load.go) | Scheduled load generation and performance reports |

`Store` defines the operations its callers need; `PostgresStore` implements those operations using PostgreSQL through pgx. `httpapi` translates HTTP into store calls, while the worker uses queue operations directly. The `http.Server` handler is the router returned by `httpapi.NewAPIHandler`. Structured `slog` output provides request and worker lifecycle logs.

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

In another terminal, start a worker with `go run ./cmd/worker` (each invocation starts one process). The API alone does not execute jobs.

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

Executed checks and remaining gaps are recorded in the [evidence index](docs/evidence.md).

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

Pass `next_cursor` unchanged as the next request's `cursor`, with the same status filter. Omit `status` to list all states. Limits range from 1 to 100. Jobs are ordered by creation timestamp and UUID. The cursor identifies the last returned `(created_at, id)` pair and status filter. The next query selects rows after that pair and fetches `limit + 1` rows to determine whether another page exists, avoiding an increasing OFFSET. Pagination traverses live data; concurrent status changes can affect filtered results between requests. Cursors are opaque navigation values, not security credentials.

Errors use `{"error":{"code":"...","message":"..."}}`. Invalid input returns 400, oversized input 413, unsupported content type 415, and a request/database deadline 503. A response may be lost after creation commits: retry with the original idempotency key. Cancellation invalidates the existing claim and clears lease ownership; the worker persistence operations check that ownership before renewal or completion. Workers detect lost ownership on heartbeat or finalization and abandon that attempt.


## Worker persistence operations

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

Design references: [PostgreSQL row locking](https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE), [database clock functions](https://www.postgresql.org/docs/18/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT).


## Run workers

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

Worker unit tests are part of `make test`. `make test-integration` also includes worker lifecycle and SIGKILL/replacement tests on a disposable PostgreSQL database. See the [evidence index](docs/evidence.md) for actual execution status.

References: [Go signal contexts](https://pkg.go.dev/os/signal#NotifyContext), [Compose configuration fragments](https://docs.docker.com/reference/compose-file/fragments/).


## Chaos harness and load evaluation

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

Experiment results are stored under a new `results/<kind>-<timestamp>-seed<seed>-<unique>/` directory every time. Each contains source/config/environment metadata, a manifest, raw JSONL events, and a final summary if the invocation reaches cleanup. Runs that start also have raw API/worker logs, per-phase HTTP counts, audit output, and (for load) a load report. Generated manifests and operation plans remain local and are ignored by Git. Submitted raw event/API/worker logs are preserved in compressed archives linked from the evidence index; their uncompressed copies are ignored. No directory is overwritten. A run without a final summary is incomplete and should be kept. The CLI exits nonzero on failed preflight, workload errors, drain timeout, invariant violations, insufficient coverage, or unmet load targets.

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

The experiment services are opt-in and use an init process for child reaping; no Docker socket mount is needed. Their workers operate only in their fresh schema, so normal workers can coexist, although running the normal stack at the same time will compete for host/database resources. For capacity evaluation, use a dedicated test deployment and record its hardware limits.

### Package experiment evidence

Run `make evidence` after experiments (requires Python 3.11+). It archives each run's raw logs and reports, including preflight failures and incomplete runs, and updates `results/archives/sha256.json`. Generated manifests and operation plans remain local. Existing archives are verified against their checksums and local originals; differing archives are rejected without overwriting. Keep local run files unchanged after packaging.

New Docker builds embed a source checksum manifest; experiment metadata records it and the harness executable SHA-256. Worker-start events record the worker executable SHA-256. Earlier recorded runs predate this identification and retain their provenance limitation. Rebuild experiment images before rerunning the corrected harness.

### Verify the deployed service and PostgreSQL failure cases

```sh
make verify-deployment
```

Requires Docker Compose 2.24.4+ (for the port reset override), Python 3.11+, and access to pull the Go test image. This uses a fresh project and disposable database volume, runs all Go tests with the race detector against PostgreSQL, and checks readiness, worker execution, and job/effect persistence after a full down/up cycle. It publishes no API port and leaves the normal deployment untouched. The disposable volume is removed during cleanup; logs and a summary remain under `results/checks/soup-verify-*`. The two added lifecycle cases observe actual lease renewal during a long job and stop/resume a worker after its claim is reassigned.

The measured eight-worker capacity failure remains part of the verdict. To investigate additional capacity, rerun the rebuilt create profile with `-workers 16`; a passing result would apply to that experiment's worker count and topology, not automatically to the four-worker deployment.
