# AI assistance

Codex reviewed the assignment PDF, proposed the implementation plan, and generated the Task 1 schema, migration runner, configuration, HTTP health endpoints, deployment files, and tests. The selected design uses PostgreSQL as the queue, following discussion with the user.

Codex's checks and environment limitations are recorded in `task-1-verification.md`. The user has not yet recorded a personal review or independent verification; this document must not be interpreted as claiming that work occurred. Update this record as implementation and human review proceed.


For Task 2, Codex implemented the PostgreSQL job store, HTTP job endpoints, cursor validation, request normalization, cancellation, unit/integration tests, and documentation. Verification and the unrun database integration checks are recorded in `task-2-verification.md`. The user has not yet recorded independent verification of these changes.


For Task 3, Codex implemented worker persistence methods, claim-version fencing, database-clock lease validation, atomic effect/completion, recovery, commit-uncertainty reporting, and targeted tests. Source review used the PostgreSQL row-locking and date/time documentation linked in the README. Executed checks and unverified concurrency cases are recorded in `task-3-verification.md`; no independent human verification is claimed.


For Task 4, Codex implemented the worker loop, environment settings, subprocess entry point, four-worker Compose configuration, heartbeat/recovery behavior, shutdown, completion reconciliation, tests, and usage documentation. Unit tests and static checks were executed; the real PostgreSQL lifecycle/SIGKILL tests and Compose runtime checks remain pending as described in `task-4-verification.md`. No independent human verification is claimed.


For Tasks 5–6, Codex implemented the chaos/load tools, independent audit, process supervisor, immutable run evidence, tests, Compose profiles, and delivery documents. Five actual CLI invocations failed before database access; they were retained and explicitly classified as preflight attempts. Codex wrote an AI-assisted reflection draft and a DON'T SHIP verdict grounded in those observed limitations. No successful 10,000-job workload, measured 100 RPS result, or independent human review is claimed. The submitter should review the design and reflection, run the pending checks in an accessible environment, and record their actual accepted/changed/independently verified work here.
