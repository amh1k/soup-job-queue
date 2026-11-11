# soup-job-queue

A durable job queue built with **Go and PostgreSQL**. An HTTP API manages jobs; independent worker processes execute them concurrently. Training is simulated with a timer and a countable database effect.

## Results

| Check | Result |
| --- | --- |
| Corrected harness: three chaos runs, 10,000 jobs/eight workers each | Passed audits; 44 worker kills total |
| Mixed traffic at 100 RPS | Passed; response p95 9.91 ms |
| 100 unique job creations/second, eight workers | Capacity failed: ~68 completions/second; final audit passed after drain |
| PostgreSQL tests with race detection | Passed with zero skips, including heartbeat and paused-worker recovery |
| Compose readiness and restart/persistence | Passed |

**Verdict: DON'T SHIP at unrestricted 100 RPS.** The default deployment has four workers; its capacity is unmeasured. Full chaos/load reruns of the corrected image are verified by executable fingerprints and matching source checksums. See [evidence](docs/evidence.md) and [verdict](docs/verdict.md).

## Start

Requires Docker with Compose v2. On WSL, enable Docker Desktop integration.

```sh
docker compose up --build -d
curl --fail http://localhost:8080/readyz
```

Compose starts PostgreSQL, applies migrations, then starts the API and **four workers**. `/healthz` checks the HTTP service; `/readyz` checks database/migration readiness. `docker compose down` preserves the database volume; adding `-v` deletes it. Included credentials are for local development.

## Architecture

```mermaid
flowchart LR
    Client["HTTP client"] -->|"create / get / list / cancel"| API["API process"]
    API --> DB[("PostgreSQL: jobs + job_effects")]
    subgraph Workers["Separate worker processes"]
        W1["Worker 1"]
        W2["Worker 2"]
        WN["Worker 3 ... N"]
    end
    W1 <-->|"claim / renew / recover / finish"| DB
    W2 <--> DB
    WN <--> DB
    Migration["Migration program"] --> DB
```

- **Queue:** `jobs` rows with `status = 'queued'`. PostgreSQL keeps job state and effects in one transactional system; no Redis is required.
- **Assignment:** workers pull available work. Clients submit jobs without choosing workers; there is no worker CRUD API.
- **Concurrency:** atomic claims use `FOR UPDATE SKIP LOCKED`, ordered by `(created_at, id)`. This gives approximate FCFS claim order; completion order can differ.
- **Processes:** each worker executes one job at a time and runs a recovery goroutine alongside its execution loop. Separate processes allow one worker to crash while others continue.

## Job lifecycle and guarantees

```mermaid
stateDiagram-v2
    [*] --> queued: Create
    queued --> running: Claim
    queued --> cancelled: Cancel
    running --> succeeded: Effect and success commit together
    running --> failed: Execution failure
    running --> cancelled: Cancellation wins
    running --> queued: Expired lease, attempts remain
    running --> failed: Expired lease, attempts exhausted
```

The API saves a queued job and responds immediately. A worker later claims it, simulates execution, and commits the effect and successful state together. **No transaction stays open during execution.**

| Mechanism | Purpose |
| --- | --- |
| Idempotency key | Matching POST retries return the same job; different input returns 409 |
| Lease + heartbeat | Ownership lasts 10s and renews every 2s; recovery scans every 1s |
| Claim version | Rejects writes from an old attempt after recovery or cancellation, even if a worker ID is reused |
| Row lock + database clock | Ownership and expiry are checked after acquiring the lock |
| Completion transaction | Inserts the unique effect and marks success atomically |
| Attempt limit | Expired work is retried up to five claims, then fails |

**Guarantee:** execution attempts may repeat; each successful job has exactly one committed PostgreSQL effect. Failed/cancelled jobs have none. External effects would need their own idempotency protocol.

Cancellation and completion lock the same row. If cancellation wins, it invalidates the claim and prevents completion; the worker notices on heartbeat/finalization. If completion wins, cancellation returns 409. A lease enables recovery after a crash; it does not prevent the crash. See the [one-page design](docs/design.md).

## Job API

| Endpoint | Behavior |
| --- | --- |
| `POST /jobs` | 201 new job; 200 matching retry; 409 conflicting input |
| `GET /jobs/{id}` | Job status/details |
| `GET /jobs?limit=10&status=queued&cursor=...` | Paginated jobs with `next_cursor` |
| `POST /jobs/{id}/cancel` | 200 queued/running/already cancelled; 409 succeeded/failed |

```sh
curl -i http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-training-1' \
  -d '{"duration_ms":100,"simulate_failure":false}'

curl http://localhost:8080/jobs/JOB_UUID
curl 'http://localhost:8080/jobs?limit=10&status=queued'
curl -X POST http://localhost:8080/jobs/JOB_UUID/cancel
```

`duration_ms`: required integer 1–5000. `simulate_failure`: optional boolean, default false. Keys: 1–128 bytes, not all whitespace. Bodies: at most 16 KiB; unknown fields rejected.

Pagination uses `(created_at, id)` keyset cursors, with limits 1–100. Pass `next_cursor` unchanged with the same status filter. Results reflect live data, not a snapshot. Errors use `{"error":{"code":"...","message":"..."}}`.

## Chaos testing

```mermaid
flowchart TB
    subgraph Harness["One harness process"]
        Submit["32 submission goroutines"] --> API["Experiment HTTP API"]
        Retry["Duplicate POST loop"] --> API
        Cancel["Cancellation loop"] --> API
        Kill["Kill ticker: every 2s"] -->|"call killRandom"| Supervisor["Supervisor goroutine"]
        Wait["cmd.Wait goroutines"] -->|"exit notifications via Go channel"| Supervisor
    end
    Supervisor -->|"start / SIGKILL through OS"| Workers["8 child worker processes"]
    Workers -.->|"OS exit status"| Wait
    API --> DB[("Isolated PostgreSQL schema")]
    Workers <--> DB
    Audit["Independent final SQL audit"] --> DB
```

The supervisor replaces planned kills; unexpected exits fail the experiment. Workers coordinate through PostgreSQL. The Go exit-notification channel exists inside the harness process. Normal deployment restarts use Docker's restart policy.

Each chaos run submits 10,000 jobs while retries, cancellations, and kills overlap for at least 30 seconds. After faults stop, the harness reconciles uncertain submissions, drains the queue, stops workers, and audits every expected job and effect. [Evidence and blind spots](docs/evidence.md) · [Silent-failure analysis](docs/silent-failures.md).

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

Load runs use 30s warm-up plus 5m measurement at 100 RPS. Mixed traffic is 50% create, 30% read, 10% list, 10% cancel; 10% of creates replay keys. Create-only sends 100 unique jobs/second. The API shares the harness process, so these measurements have topology/resource limits. Run capacity tests on a dedicated deployment to avoid contention from other services.

Each invocation retains a fresh database schema and writes to a unique `results/` directory. Generated input plans stay local and are ignored by Git. Run **`make evidence`** with Python 3.11+ to archive raw logs/reports and verify checksums without overwriting earlier archives.

## Verification

```sh
make build test vet
go test -race ./...
make verify-deployment
```

`verify-deployment` requires Python 3.11+ and Compose 2.24.4+. It runs the complete race-test suite against PostgreSQL, then checks real worker execution and persistence across down/up. It uses a disposable project, publishes no API port, and removes its own volume afterward. Logs remain under `results/checks/`.

Without `TEST_DATABASE_URL`, ordinary Go tests skip database cases. For an existing disposable database:

```sh
TEST_DATABASE_URL='postgres://soup:soup@localhost:5432/soup_test?sslmode=disable' \
  make test-integration
```

## Local development

Requires Go 1.26+ and PostgreSQL 18. Set `DATABASE_URL`, then run these programs in separate terminals after migrations:

```sh
export DATABASE_URL='postgres://soup:soup@localhost:5432/soup?sslmode=disable'
go run ./cmd/migrate
go run ./cmd/api
# In another terminal, with DATABASE_URL set:
WORKER_ID=local-worker go run ./cmd/worker
```

Defaults: HTTP `:8080`, database timeout `3s`, pool size `10` per process (Compose workers use `4`), idle polling `100ms`, recovery batch `100`. Worker timing is configurable through `LEASE_DURATION`, `HEARTBEAT_INTERVAL`, `POLL_INTERVAL`, and `RECOVERY_INTERVAL`; heartbeat interval plus database timeout must be less than the lease duration.

## Deliverables and code

| Deliverable | Location |
| --- | --- |
| Design note | [docs/design.md](docs/design.md) |
| Run counts, raw results, and limitations | [docs/evidence.md](docs/evidence.md) · [archives](results/archives) |
| Silent failures | [docs/silent-failures.md](docs/silent-failures.md) |
| Production verdict and fixes | [docs/verdict.md](docs/verdict.md) |
| Reflection draft | [docs/reflection.md](docs/reflection.md) |
| AI disclosure | [docs/ai-usage.md](docs/ai-usage.md) |

Entry points: [`cmd/`](cmd). HTTP routes: [`internal/httpapi`](internal/httpapi). PostgreSQL operations: [`internal/jobs`](internal/jobs). Execution loop: [`internal/worker`](internal/worker). Harness: [`internal/experiment`](internal/experiment). Independent checks: [`internal/audit`](internal/audit). Schema: [`migrations`](migrations).
