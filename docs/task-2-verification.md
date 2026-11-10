# Task 2 verification

Implemented job creation, get, list, and cancellation using the existing schema. The API is connected to the real PostgreSQL store. No schema changes were required.

## Decisions

- Creation inserts with a unique idempotency key and `ON CONFLICT DO NOTHING`. A separate statement reads the winning row after a conflict, providing a fresh READ COMMITTED snapshot. Comparison uses normalized typed input, so field order and an omitted false default do not cause a conflict.
- Pagination fetches `limit + 1` rows in ascending `(created_at,id)` order. The cursor carries the final returned timestamp, UUID, and status filter. Invalid or differently filtered cursors are rejected.
- Cancellation reads the job with `FOR UPDATE`, rejects succeeded/failed jobs, preserves repeated cancellations, and invalidates the claim when cancelling queued/running jobs. Final worker transactions will acquire this same lock in Task 3.
- Database operations receive the request deadline. Cancellation rollback has a separate bounded cleanup context.

## Checks performed

- `go test -race -count=1 ./...` passed executable tests. Database integration tests skipped because `TEST_DATABASE_URL` was unset.
- HTTP unit tests cover input limits, missing/malformed/null fields, unknown fields, trailing JSON, default normalization, creation/replay status codes, error mapping, cursor round trips, changed filters, invalid IDs, cancellation routing, deadlines passed to creation, and method rejection.
- UUID validation includes a decoded-length check, preventing extra hyphens from passing validation after stripping separators.
- `make build vet` passed.
- PostgreSQL 18.6 single-user mode accepted the actual create/cancel SQL and representative parameterized cursor SQL. A smoke sequence checked one retained row after duplicate insertion and cancellation with claim invalidation. This mode does not exercise pgx scanning, multiple connections, or concurrent transactions.

## Pending checks

Docker remains unavailable through the WSL Docker Desktop command. This sandbox disallows database sockets, as recorded during Task 1. Full integration tests are included for:

- 100 concurrent creates sharing a key, yielding one row and one ID.
- Different input with the same key returning a conflict.
- Retrying creation after cancellation returning the original cancelled job.
- Pagination with equal creation timestamps, empty results, and changed cursor filters.
- Cancelling running, cancelled, terminal, and missing jobs.
- HTTP replay with omitted/explicit defaults after discarding the initial response.

Run `TEST_DATABASE_URL=... make test-integration` against a disposable PostgreSQL database to verify these cases. Until then, database concurrency correctness is an implementation claim backed by design and tests awaiting execution, not a measured result. Worker execution and crash recovery remain future tasks.
