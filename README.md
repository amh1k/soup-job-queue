# soup-job-queue

A training-job queue backed by PostgreSQL. See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) for the design and remaining tasks.

## Current implementation

Task 1 provides the schema, atomic checksum-verified migrations, shared job types and store interface, environment configuration, and API health checks. Job endpoints and workers come in subsequent tasks.

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
make vet
TEST_DATABASE_URL='postgres://soup:soup@localhost:5432/soup_test?sslmode=disable' make test-integration
```

Integration tests require a **disposable database** whose user can create schemas. Each test creates and drops its own schema. They cover readiness before/after migrations, data preservation on reruns, migration checksums, constraints, and indexes. `go test ./...` skips integration tests if `TEST_DATABASE_URL` is absent; the explicit Make target refuses to run without it.

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
