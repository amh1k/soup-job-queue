# Task 4 verification

Implemented `cmd/worker`, the worker loop, environment configuration, Docker image binary, and four independently identified Compose worker services. No schema changes were required.

## Runtime decisions

- One active job per worker. Simulation uses a cancellable timer; only the database finalization transaction creates an effect.
- Heartbeats use bounded calls. Any renewal failure stops the attempt, leaving recovery to decide its future. Terminal writes still validate database ownership and lease state.
- Expired-job scans run independently from execution. Recovery errors are retried on the next scan. Claim failures use jitter and backoff; idle polls use jitter.
- SIGINT/SIGTERM cancels the runtime and waits for its recovery goroutine. Unfinished work is abandoned with its lease intact for safe recovery.
- A lost completion commit is inspected via a fresh bounded `Get`. Matching status and claim version confirm the transition; unresolved results are logged and abandoned without repeating the effect. Invariant violations stop the worker.
- Startup checks migrations. Compose restarts failed worker processes; the API and migration commands remain separate programs.

## Executed checks

- `go test -race -count=1 ./...` passed executable tests, including the worker unit suite.
- `make build vet` passed for API, migration, and worker programs.
- Unit tests cover successful execution with heartbeats and recovery scans, simulated failure, ownership/database/timeout heartbeat errors, shutdown, transient claim/recovery errors, uncertain completion reconciliation, infrastructure completion errors, invariant failures, and configuration validation.
- Compose YAML parsed with its anchors resolved. Static checks confirmed four worker services, unique IDs, inherited connection limits/settings, worker commands, and migration completion dependencies.

## Pending deployment and integration checks

PostgreSQL socket access still fails with `Operation not permitted`, and Docker Desktop is unavailable in this WSL distribution. Consequently the following real-database tests are included but unrun:

- Worker lifecycle: success with one effect, simulated failure with no effect, cancellation during execution with no effect, and subsequent work completing.
- SIGKILL recovery: run the actual worker runtime in an independent test subprocess, kill it while a job is running, then start a replacement and verify at least two attempts but exactly one effect. The subprocess exercises the runtime, not the `cmd/worker` entry point or Docker restart policy.

Run `TEST_DATABASE_URL=... make test-integration` against an accessible disposable PostgreSQL database. Then verify `docker compose up --build`, API create/get/list/cancel flows, all four worker logs, graceful stops, and container restarts. Configuration parsing and unit test success are not evidence that those deployment checks passed.

The 10,000-job chaos harness, its preserved run output, the 100 RPS evaluation, and the shipping verdict remain Tasks 5–6.
