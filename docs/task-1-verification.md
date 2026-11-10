# Task 1 verification

Implemented: Go module, configuration, job model/store interface, PostgreSQL schema and migration command, liveness/readiness endpoints, graceful shutdown, Dockerfile, Compose startup dependencies, Make targets, and usage documentation.

## Checks performed

- Tests were introduced before implementation. The initial run failed because `Load` and `NewHandler` did not exist; migration tests also initially lacked dependency checksums.
- Unit tests cover configuration defaults and invalid settings, liveness without a database call, readiness success/failure, non-disclosure of database errors, and a readiness query deadline.
- `go test -race -count=1 -v ./...` passed for executable unit tests with no reported memory races. The two PostgreSQL integration tests explicitly skipped because `TEST_DATABASE_URL` was unset.
- `make build vet` passed using Go 1.26.8.
- `go mod verify` passed. Dependencies were resolved from the existing local module cache, with writable build/module caches under `/tmp` because of filesystem restrictions.
- PostgreSQL 18.6 single-user mode accepted the schema. Ten invalid-input cases verified SQLSTATE rejection for invalid status, empty/oversized keys, malformed ownership/completion state, invalid attempt/version values, and orphan effects. Additional checks rejected duplicate keys and duplicate effect rows.
- A second PostgreSQL single-user invocation found the original inserted row, checking local database persistence across backend restarts.
- Compose YAML parsed successfully. Its migration completion dependency and PostgreSQL named-volume path were checked statically.

## Pending checks and limitations

- Docker is unavailable in this WSL distribution, so the container image build, service health checks, startup order, and named-volume persistence were not exercised.
- The sandbox denies both TCP and Unix socket operations (`Operation not permitted`). Attempts to start a temporary PostgreSQL server and connect to the installed server failed. The single-user checks above do not verify pgx transactions or HTTP/database integration.
- The integration test suite is included in `migrations/migrations_test.go` but skips without `TEST_DATABASE_URL`. It still needs to run against an accessible disposable database. This covers migration reruns with retained data, checksum rejection, and readiness before/after migration in addition to schema constraints/indexes.
- No worker, job API, chaos harness, throughput measurement, or shipping verdict has been implemented in Task 1.

Run the README verification commands in an environment with Docker or an accessible PostgreSQL server to complete the pending checks. Do not interpret the static Compose and single-user checks as full deployment verification.
