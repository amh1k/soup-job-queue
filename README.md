# soup-job-queue

A training-job queue backed by PostgreSQL.

## Current implementation

Tasks 1–2 provide the schema, atomic checksum-verified migrations, configuration, health checks, and the job API. Jobs stay queued until workers are implemented in Tasks 3–4.

## Start with Docker Compose

Requires Docker Engine/Desktop with Compose v2. For WSL, enable Docker Desktop integration for this distribution.

```sh
docker compose up --build
curl --fail http://localhost:8080/healthz
curl --fail http://localhost:8080/readyz
```

Compose waits for PostgreSQL, applies migrations, then starts the API. The database uses a named volume; `docker compose down` preserves data. `docker compose down -v` **deletes** that data. The included database credentials are for local development.

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
docker compose exec db psql -U soup -d soup -c "INSERT INTO jobs (idempotency_key, request) VALUES ('persistence-check', '{}') ON CONFLICT DO NOTHING"
docker compose down
docker compose up --build -d
docker compose exec db psql -U soup -d soup -c "SELECT id, status FROM jobs WHERE idempotency_key = 'persistence-check'"
```

Wait for the database to become healthy before the final query. The expected result is the original row, still queued.

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

Errors use `{"error":{"code":"...","message":"..."}}`. Invalid input returns 400, oversized input 413, unsupported content type 415, and a request/database deadline 503. A response may be lost after creation commits: retry with the original idempotency key. Cancellation invalidates the existing claim and clears lease ownership; worker cooperation and completion races will be implemented in Task 3.

Task 2 verification and pending database checks: [docs/task-2-verification.md](docs/task-2-verification.md).
