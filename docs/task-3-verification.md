# Task 3 verification

Implemented `ClaimNext`, `Renew`, `Finish`, `Fail`, and `RecoverExpired`, plus a `Queue` interface for future worker processes. The existing schema and API remain compatible; no migration was changed.

## Transaction decisions

- Claiming uses a single atomic update with a locked, ordered subquery. Locked jobs are skipped and each successful claim increments both version and attempt count.
- Renewal, completion, and failure use short READ COMMITTED transactions. Each locks the job, validates current owner/version, and checks PostgreSQL time after acquiring the lock. Caller-provided timestamps cannot revive expired ownership. The row lock reserves the validated transition through commit.
- Completion inserts the unique effect and updates success together. Duplicate effects are treated as invariant failures, not hidden by conflict handling. Repeated successful completion is rejected as ownership lost.
- Recovery locks bounded batches, rechecks expiry, clears ownership, increments the version, preserves creation time, and fails exhausted jobs instead of retrying forever.
- A transport error during COMMIT can have an unknown outcome. Such errors carry `ErrCommitUnknown` so the future worker can reconcile persisted state on a fresh connection. No network-failure reconciliation run has been performed yet.

## Executed checks

- `go test -race -count=1 ./...` passed executable unit tests. PostgreSQL integration cases skipped without `TEST_DATABASE_URL`.
- `make build vet` passed.
- PostgreSQL 18.6 single-user mode accepted the actual SQL constants for claim, renewal, effect insert, success, failure, expired selection, and recovery. A smoke sequence verified claim metadata, renewal, rollback of both effect and success, committed success with one effect, requeue with an invalidated version, and terminal failure at the attempt limit.
- Input unit tests cover invalid lease durations, empty worker IDs, invalid claim identifiers/versions, invalid failure input, and recovery batch bounds.

## Integration tests included, awaiting execution

- Eight simultaneous claimers receive different jobs; attempts increment once; an empty queue returns nil.
- Claiming skips a locked oldest job and returns to it after unlock.
- Stale owners/versions and expired claims cannot renew, finish, or fail a job; reusing a worker ID does not validate an old attempt.
- Artificially incorrect caller timestamps do not change database lease authority. This checks distrust of the caller's snapshot, not actual OS/database clock-jump fault injection.
- A lease that expires while renewal waits for a lock cannot be revived.
- A trigger-injected failure after effect insertion rolls back both the effect and terminal state. This tests atomic rollback, not a real SIGKILL.
- Duplicate pre-existing effects surface an invariant violation.
- Execution failure and exhausted leases produce failed jobs without effects.
- Concurrent bounded recovery loops recover each expired job once and preserve healthy leases; locked expired jobs are skipped.
- Cancellation fences all worker writes. Twenty-four finish/cancel races accept only succeeded-with-one-effect or cancelled-with-zero-effects.

The sandbox still rejects database socket connections with `Operation not permitted`. Single-user SQL checks cannot verify pgx scanning or multi-connection races. Run `TEST_DATABASE_URL=... make test-integration`, then the repeated race-enabled jobs test from the README on an accessible disposable PostgreSQL database before treating concurrency behavior as verified.

No worker process, SIGKILL chaos harness, or performance verdict is implemented yet; those remain subsequent tasks.
