# Training Job Queue Implementation Plan

> For agentic workers: implement task-by-task using the writing-plans execution workflow. Referenced execution skills may need installation; this plan does not authorize implementation or delegation.

**Goal:** Build a job API and concurrent workers, then produce honest evidence of correctness under crashes and retries.

**Architecture:** PostgreSQL stores the queue, leases, and simulated execution effects. Independent worker processes claim jobs with short transactions, execute outside transactions, then atomically commit an effect and terminal state after validating ownership.

**Tech stack:** Proposed: Go, standard-library HTTP server, pgx, PostgreSQL, Docker Compose. Go is a recommendation, not a user-selected requirement; the design also works in other languages.

**Spec:** `Soup-Task-04-Backend-Engineer(1).pdf` in this directory. This document contains the design and implementation sequence. Task 1 code is implemented; database integration and container verification remain pending due to environment restrictions. See `docs/task-1-verification.md`. No benchmark results exist yet.

## 1. Requirements and boundaries

- Start the system with `docker compose up`.
- Create with an idempotency key; get status; paginate lists; cancel.
- States: queued, running, succeeded, failed, cancelled.
- At least four workers normally; eight in the chaos harness.
- Emulate execution with a countable database row write; real model training is unnecessary.
- Chaos workload: 10,000 jobs, repeated same-key POSTs, cancellations, and random worker SIGKILL roughly every two seconds.
- Check no lost jobs, duplicate effects, stuck running jobs, or inconsistent terminal states.
- Preserve raw output from every run, including failures; state run counts and blind spots.
- Explain double claims, zombie workers, cancellation races, retry duplicates, transaction isolation, and clock skew.
- Deliver a one-page design note, SHIP/DON'T SHIP verdict at 100 requests/second, reflection, and AI-use disclosure.
- No frontend, worker-management API, authentication system, priority scheduler, or separate message broker is required by the brief.

## 2. Main decisions

### PostgreSQL instead of SQLite

Choose PostgreSQL. Row locks and `FOR UPDATE SKIP LOCKED` make concurrent claims straightforward. Transactions can protect both the effect and final state. Constraints enforce unique request keys and effects across every process. SQLite can work, but its single-writer behavior makes this particular concurrency exercise less natural. PostgreSQL adds a container and operational complexity; that is an acceptable tradeoff here.

### Database queue instead of Redis or a broker

Use one durable authority. A separate broker introduces coordination between queue acknowledgment and database commit without helping the assignment's core invariant. Revisit a broker only after measuring a concrete queue bottleneck.

### Scheduling: approximate FCFS

Select eligible queued jobs by `(created_at, id)` and skip rows locked by another worker. This is best-effort first-come-first-served claim order. It does not guarantee strict start or completion order: workers run concurrently, locks are skipped, and retries return to the queue.

The PDF does not require multiple scheduling policies. Implement one documented policy; omit priority, round-robin, shortest-job-first, and policy abstractions. Expired jobs keep their original creation time, with a finite attempt limit preventing a broken job from retrying forever. Strict FIFO would reduce concurrency and still would not ensure FIFO completion.

### Guarantee: retried attempts, at most one committed effect

A crashed attempt may execute again. For a successful job, exactly one simulated effect row and the succeeded state commit in the same PostgreSQL transaction. Failed and cancelled jobs have zero effect rows. Describe this as at-least-once attempts with an exactly-once database effect for successful jobs, not exactly-once computation.

This guarantee is scoped to a row in this database. An external API call or file write cannot automatically share that transaction; it would require its own idempotency mechanism or an outbox and idempotent consumer.

## 3. Data model and invariants

`jobs`:

- UUID primary key; unique non-null `idempotency_key`.
- Validated normalized request JSON, stored for duplicate-request comparison.
- Status constrained to the five states.
- `created_at`, `updated_at`, `completed_at` using database timestamps.
- Nullable `worker_id`, `lease_expires_at`; integer `claim_version` initially zero.
- `attempt_count` initially zero, `max_attempts` default 5.
- Nullable structured failure code and message.

`job_effects`:

- `job_id` primary key and foreign key to jobs.
- `claim_version`, result JSON, and database creation timestamp.
- Do not silently swallow duplicate-effect conflicts: treat an unexpected conflict as an invariant violation.

Indexes: partial `(created_at, id)` for queued jobs; partial `(lease_expires_at, id)` for running jobs; `(created_at, id)` for listing. Add status-filter index only if query measurements justify it.

Database checks enforce that running jobs have ownership and lease fields, other states do not, and terminal states have completion timestamps. Cross-table invariants are enforced by transactions and independently audited by tests.

## 4. API contract

| Endpoint | Behavior |
|---|---|
| `POST /jobs` | Require `Idempotency-Key`; accept `duration_ms` (1–5000) and `simulate_failure` (boolean, default false). Return 201 for creation, 200 for a matching replay, 409 when a key is reused with different normalized input. |
| `GET /jobs/{id}` | Return job state, attempts, timestamps, and failure information; 404 if absent. |
| `GET /jobs?limit=50&cursor=...&status=...` | Keyset pagination in ascending `(created_at,id)` order; limit 1–100; optional validated status. |
| `POST /jobs/{id}/cancel` | Queued/running becomes cancelled; repeat cancellation returns 200; succeeded/failed returns 409; unknown job returns 404. |
| `GET /healthz` | Process liveness. |
| `GET /readyz` | Database reachable and migrations applied. |

Reject malformed JSON, unknown fields, invalid UUIDs/cursors/statuses, empty or over-128-byte keys, and bodies larger than 16 KiB. Use explicit defaults before comparing request JSON so omitted defaults and explicit defaults match. Return consistent JSON errors and bounded request/database timeouts.

Creation uses `INSERT ... ON CONFLICT DO NOTHING`, followed by a fresh statement to read and compare the existing job. Under READ COMMITTED this handles concurrent inserts without unsafe check-then-insert logic. Retain keys with jobs; cancellation does not release a key.

Cursor encodes the last creation timestamp, ID, and filter. Reject reuse with a different filter. Pagination is a live traversal, not a snapshot of mutable statuses; document that limitation.

## 5. Worker protocol

Each worker is an independent OS process with one active job, so killing one does not kill the API or other workers. Docker Compose starts four explicitly named worker services from the same image.

Initial configurable values: lease 10 seconds, heartbeat 2 seconds, recovery scan 1 second, idle polling 100 ms with jitter, maximum 5 attempts. These are starting values to validate, not measured optimal settings.

### Claim

In a short READ COMMITTED transaction, select one queued job ordered by creation and ID using `FOR UPDATE SKIP LOCKED LIMIT 1`. Update it to running, assign worker ID, increment claim version and attempts, and set lease expiry from PostgreSQL time. Commit before simulating work. Return `Claim{Job, WorkerID, Version}`.

### Heartbeat

Extend the lease only when status is running, worker and version match, and the old lease has not expired. Use database time. A zero-row update means ownership is lost: stop work and never commit an effect. On database errors, stop attempting completion until ownership can be safely validated; bounded timeouts prevent indefinite blocking.

### Finish

Begin a transaction and lock the job row. After acquiring the lock, read actual database time (`clock_timestamp()`), then verify running state, worker ID, claim version, and unexpired lease. On success insert the effect, mark succeeded, clear ownership, and commit together. A stale claim returns a typed ownership-lost result with no write.

For a simulated execution failure, validate ownership in the same way, mark failed, and write no effect. If the transaction commit result is uncertain because the connection dropped, reconnect and inspect persisted job/effect state; never assume rollback or repeat an external side effect.

### Recovery

Each worker periodically scans a bounded batch of expired running jobs using row locks and SKIP LOCKED. Recheck expiry after locking. Requeue if attempts remain; otherwise mark failed with `attempts_exhausted`. Clear ownership and invalidate the claim version. Multiple recovery loops are safe because they lock and recheck the same rows.

Recovery requires a surviving or restarted worker and an available database. No system can promise eventual completion while all execution capacity remains down.

### Cancellation

Lock the job row and transition queued/running directly to cancelled, clearing ownership and invalidating its version. The worker detects this on its heartbeat or final check. Work may continue briefly in memory, but it cannot commit an effect.

Cancellation and completion serialize on the same row lock. If completion commits first, cancellation returns 409. If cancellation commits first, completion is rejected. Cancellation cannot undo an already committed success.

### Clock and isolation decisions

Use PostgreSQL time for all lease decisions; host wall-clock differences cannot grant ownership. Use monotonic local timers only for sleeping and heartbeat scheduling. READ COMMITTED plus row locking, conditional writes, and unique constraints is sufficient for these operations. Avoid holding a transaction open during execution. A database clock jump can affect recovery timing, so do not claim immunity to every form of clock skew.

## 6. Repository layout

```text
cmd/api/main.go                 HTTP service entry point
cmd/worker/main.go              worker process entry point
cmd/migrate/main.go             schema migration command
cmd/chaos/main.go               process supervisor and workload harness
cmd/load/main.go                paced 100 RPS workload
internal/config/config.go       validated environment settings
internal/jobs/model.go          types, states, errors, normalized input
internal/jobs/store.go          create, read, list, cancel
internal/jobs/claims.go         claim, renew, finish, recover
internal/httpapi/handlers.go    routing and request/response validation
internal/worker/worker.go       polling, execution, heartbeat, shutdown
internal/audit/audit.go         independent invariant queries
migrations/001_initial.sql      schema and indexes
internal/jobs/store_test.go     real-Postgres integration tests
internal/jobs/claims_test.go    races, expiry and fencing tests
internal/httpapi/handlers_test.go API contract tests
internal/worker/worker_test.go  worker lifecycle tests
tests/chaos_test.go             harness validation
Dockerfile / compose.yaml      image, DB, migration, API, four workers
Makefile                       reproducible commands
docs/design.md                 required one-page design note
docs/verdict.md                 measured ship decision and limitations
docs/ai-usage.md                actual AI contributions and verification
results/<run-id>/               immutable raw evidence per invocation
README.md                      setup, commands, contract, evidence index
```

## 7. Implementation tasks

Each task: write the specified failing tests, observe failure, implement, run focused checks, review the diff, then commit a coherent change. All database concurrency tests use real PostgreSQL, not database mocks.

### Task 1 — Establish schema and runnable API

- [x] Create configuration, migration command, schema, Dockerfile, Compose DB/migration/API services, and health handlers.
- [x] Define `Job`, `CreateInput`, `Claim`, `Page`, `Cursor`, and typed errors in `internal/jobs/model.go`.
- [x] Define store interfaces: `Create(ctx,key,input) (Job,bool,error)`, `Get(ctx,id) (Job,error)`, `List(ctx,limit,cursor,status) (Page,error)`, `Cancel(ctx,id) (Job,error)`. The boolean means newly created.
- [ ] Test schema constraints and readiness before/after migration; migrations must be repeatable without destroying data.
- [ ] Check clean `docker compose up --build` reaches readiness and DB data survives restart.

### Task 2 — Job API and idempotency

- [ ] Add tests: 100 concurrent creates with one key produce one ID and one row; changed input returns conflict; matching retry after cancellation returns the original cancelled job.
- [ ] Add API validation tests and pagination tests with equal timestamps, invalid cursors, status filters, and boundary limits.
- [ ] Implement store queries and HTTP contract from section 4.
- [ ] Verify a timed-out client can retry a committed creation with the same key and recover the same ID.
- [ ] Run `go test ./internal/httpapi ./internal/jobs` against the test database; all cases must pass.

### Task 3 — Claims, fencing, recovery, and cancellation races

- [ ] Define `ClaimNext(ctx,workerID) (*Claim,error)`, `Renew(ctx,claim) error`, `Finish(ctx,claim,result) error`, `Fail(ctx,claim,reason) error`, and `RecoverExpired(ctx,limit) (int,error)` in `claims.go`.
- [ ] Test eight simultaneous claimers: each current claim has exclusive ownership, attempts increment once per successful claim, locked jobs are skipped.
- [ ] Test stale version and expired lease completion/renewal rejection, including a worker paused past expiry and resumed after reassignment.
- [ ] Test simultaneous finish/cancel: exactly one legal outcome, never cancelled plus an effect.
- [ ] Test rollback between effect insertion and status update leaves neither committed; retry after successful commit creates no second effect.
- [ ] Test recovery retry limits, concurrent recovery loops, and application clocks offset in either direction while database lease behavior remains unchanged.
- [ ] Implement the short transactions from section 5 and run `go test ./internal/jobs -count=10`.

### Task 4 — Worker runtime and default deployment

- [ ] Implement one job at a time per worker, cancellable simulated execution, periodic renewal/recovery, idle backoff, and structured logs with job ID, worker ID, claim version, and attempt.
- [ ] Add four worker services sharing the image; migration completion gates service startup.
- [ ] Test worker shutdown, transient DB loss, ownership loss during execution, simulated failure, and process death followed by lease recovery.
- [ ] Run `go test -race ./...`; this checks Go memory races, while database tests check transaction races.
- [ ] Verify normal create/get/list/cancel flows through the running Compose deployment.

### Task 5 — Chaos harness and independent audit

- [ ] Build a Go supervisor that starts exactly eight worker child processes, retains PIDs, SIGKILLs a random live worker roughly every two seconds, reaps it, and starts a replacement. Docker runs the harness with an init process; no Docker socket is required.
- [ ] Use an isolated database per run or a dedicated disposable Compose test project. Never reset a normal deployment's data.
- [ ] Persist a manifest of 10,000 unique keys and normalized payloads before submission. Use a recorded seed; choose short deterministic durations so the run finishes within a bounded drain period.
- [ ] Run producers, duplicate request loops, cancellation requests, and the kill loop concurrently. Persist every response/error and reconcile ambiguous timeouts by retrying the same keys.
- [ ] Stop fault injection and producers, keep eight healthy workers, and drain with a configurable five-minute deadline. Timeout is a failed run, not evidence to discard.
- [ ] Audit database state independently: exactly one job per manifest key; no unexpected jobs; no nonterminal jobs after drain; succeeded iff exactly one effect; failed/cancelled have zero effects; no orphan effects or residual terminal ownership. Every accepted cancellation must remain cancelled.
- [ ] Include deliberate simulated failures in the workload so failed-state invariants are exercised. Audit attempts exhausted separately from requested execution failures.
- [ ] Record seed, commit and dirty-diff metadata, settings, environment, start/end times, submitted/accepted counts, kill count, retries, cancellations, terminal counts, exit codes, and all worker/API/harness output.
- [ ] Use unique run directories and never overwrite failed evidence. Flush logs continuously so interrupted runs retain useful output.
- [ ] Validate the audit itself with intentionally corrupted disposable fixtures; it must detect each invariant violation.
- [ ] Run at least three full 10,000-job runs with different seeds; preserve all actual invocations, including development failures, and report actual successful/failed/incomplete counts. More runs improve evidence but do not prove correctness.

### Task 6 — 100 RPS evaluation and delivery

- [ ] Create a paced open-loop load driver targeting 100 total HTTP requests/second, recording scheduled vs actual send times so slow responses do not hide overload.
- [ ] Define a reproducible workload: 50% creation, 30% reads, 10% lists, 10% cancellations; record duplicate-key fraction and duration distribution. Also test 100 creates/second because the brief does not define request mix.
- [ ] Warm up for 30 seconds, measure for five minutes, stop submissions, then drain. Record throughput, p50/p95/p99 latency, errors, queue age/backlog, DB pool waits, attempts, and resource limits.
- [ ] Explain capacity: eight workers with 100 ms execution average have an ideal ceiling near 80 completions/second before overhead. API throughput alone cannot demonstrate a sustainable arrival rate.
- [ ] Use proposed acceptance targets, clearly labeled as our targets: zero invariant violations, zero unexpected server errors, p95 API latency under 250 ms, achieved send rate within 5% of target, and no sustained backlog growth for the declared workload. Report failures honestly.
- [ ] Write the one-page design note, measured SHIP/DON'T SHIP verdict, first fixes if needed, reflection, and actual AI-use disclosure. Do not invent personal observations or claim unrun verification.
- [ ] Add README commands for normal startup, integration tests, chaos runs, and load runs, plus an evidence index including failed runs.
- [ ] Perform final requirement-to-artifact review and a clean-environment startup check.

## 8. Evidence matrix and blind spots

| Risk | Targeted evidence |
|---|---|
| Double claim | Concurrent real-DB claims with ownership assertions |
| Zombie worker | Pause past expiry, reassign, resume, reject old token |
| Cancel/complete race | Synchronize competing transactions; inspect state and effect |
| Retried request | Concurrent identical keys, mismatched payloads, lost-response retry |
| Isolation mistakes | READ COMMITTED integration races using separate connections |
| Clock skew | Offset application clocks; all lease decisions remain in DB |
| Crash during finalization | Kill before commit and after commit; inspect persisted outcomes |
| Missed jobs | Manifest-to-database reconciliation, not just aggregate counts |

Random SIGKILL testing does not exhaust every interleaving. It does not establish database failover safety, storage durability after host power loss, network-partition behavior, external-effect guarantees, long-term table growth, security, fairness under an infinite workload, or capacity on different hardware. Targeted tests cover selected race windows that random kills may never hit. SIGSTOP/SIGCONT is useful for zombie behavior because a killed process cannot return.

## 9. Recommended delivery order

First make the invariants and transaction boundaries correct, then implement the API and worker runtime, then collect failure evidence and performance measurements. Keep the initial version small enough to explain in the live discussion. The brief estimates 4–6 hours of work inside a 48-hour deadline; this detailed validation plan may take longer depending on tooling and failures. Prioritize the required design, durable protocol, full chaos runs, and honest verdict before optional observability or extra scheduling features.
