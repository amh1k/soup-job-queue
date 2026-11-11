# AI usage disclosure

## Where AI was used

Codex helped interpret the assignment, explain concurrency concepts, propose the design, and implement the API, PostgreSQL store, migration runner, worker runtime, deployment configuration, tests, chaos/load tools, and independent audit. Codex also drafted and revised the README and delivery documents, inspected saved experiment reports, and packaged raw logs into archives.

## Decisions accepted or changed

Following discussion, the implementation uses Go and PostgreSQL with the database acting as the queue. The user chose to keep PostgreSQL as the queue after considering Redis. The implementation uses row locking, leases, claim versions, and a transaction covering the database effect and successful completion. These choices and their limits are explained in [design.md](design.md).

The user questioned the large generated manifests in Git. They were removed from tracking and ignored; planned inputs remain reproducible from the generator, seed, and options. Original raw experiment logs are preserved in compressed archives. The user also requested clearer architecture diagrams and documentation focused on the assignment deliverables.

## What was actually verified

The user ran three 10,000-job chaos seeds and both 100 RPS load profiles through Docker Compose. Codex inspected the persisted summaries and audits: three chaos passes, a mixed-load pass, and a create-only capacity failure with a passing final audit. The failure is retained and informs the DON'T SHIP verdict.

Codex ran Go unit/race tests, build/vet checks, and SQL smoke checks. Earlier sandbox checks skipped nineteen targeted PostgreSQL tests. The user subsequently ran `make verify-deployment`: the complete PostgreSQL race-test suite passed with zero skips, and normal deployment readiness and restart/persistence passed. Sufficient capacity for 100 unique creates/second remains unverified. Full results, earlier failures, and limitations are in [evidence.md](evidence.md).

The reflection is an AI-assisted draft for the submitter to review. No independent human code review or manually rewritten implementation is claimed; the user-executed Docker runs are the recorded human verification activity.

Following a source/evidence audit, Codex corrected cancellation error handling and final evidence reporting, added executable/source identification, and added a repeatable evidence archive command. Existing measurements were preserved. Targeted regression checks were added; fresh Docker workloads are required for the changed harness.

Codex subsequently added PostgreSQL lifecycle tests for long-running heartbeat renewal and an actual paused/resumed child worker, plus an isolated Compose verification command for the full suite and restart/persistence. Docker access remained restricted for Codex, but the user subsequently ran these checks successfully in their normal terminal; Codex inspected the command logs, zero-skip PostgreSQL test output, and persisted job/effect results.
