# Silent failure analysis

| Risk | Protection | Check and actual evidence status |
| --- | --- | --- |
| Double claim | Ordered locked selection with SKIP LOCKED; claim and state change in one statement | Eight concurrent claimers and locked-oldest tests are implemented in `internal/jobs/claims_test.go`; The PostgreSQL integration cases passed in the full deployment verification suite. Claim SQL passed single-user smoke checks. |
| Zombie worker | Owner plus monotonically increasing version and current lease are checked under a row lock | Expire/reassign/resume tests reject old finish/fail/renew, including reused worker IDs. The integration cases and actual SIGSTOP/reassignment/SIGCONT process test passed; the resumed attempt was rejected. |
| Cancel versus complete | Both operations serialize on the same job row | Twenty-four real-database race iterations and deterministic cancellation-first cases passed against PostgreSQL. The independent audit rejects cancelled jobs with effects. |
| Duplicate POST retry | Unique key; separate read of winning committed row; normalized input comparison | 100 concurrent creates and lost-response replay tests passed against PostgreSQL. HTTP normalization and error tests pass. |
| Partial effect/final state | One finalization transaction and unique effect row | Trigger-injected rollback test passed against PostgreSQL. Actual transaction SQL passed single-user rollback/commit checks. The audit rejects success without an effect, duplicate effects, and failed/cancelled effects. |
| Isolation assumptions | READ COMMITTED explicit worker transactions, locks before validation, no long execution transaction | Concurrency tests use separate real connections and a 16-connection pool; these cases passed against PostgreSQL. Go race detection passed, which does not establish database isolation correctness. |
| Lease expires during lock wait | Read actual database time after obtaining ownership lock | A lock-wait expiry integration case passed against PostgreSQL. |
| Clock skew | Worker wall clocks and cached lease timestamps do not authorize writes; PostgreSQL time does | Wrong cached timestamps are integration fixtures that passed against PostgreSQL. Database clock jumps and OS-level skew injection remain untested; they can affect recovery timing. |
| Unknown COMMIT outcome | Fresh read of terminal state/version; abandon unresolved outcomes without duplicating effects | Worker unit tests pass for reconciliation. A real network drop during COMMIT has not been injected. |
| Harness misses lost jobs | Expected-key and payload reconciliation instead of aggregate totals alone | Pure audit corruption tests pass; an integration fixture removes an effect and adds an orphan, and passed against PostgreSQL. |

Three subsequent full chaos runs passed their independent final audits under concurrent claims, retries, cancellations, and 14 worker kills per run. Both load-run audits passed; create-only capacity failed. These end-to-end observations supplement, but do not replace, the targeted integration cases, which also passed in the subsequent full PostgreSQL race-test suite with zero skips. See [the evidence index](evidence.md).

The tests themselves are inspectable evidence of what is intended to be checked. Unrun tests are not proof that the protection works under the stated condition. Raw check output is linked from `evidence.md`.

The chaos harness kills and replaces workers while duplicate POSTs and cancellation requests run alongside creation. Its independent final audit is stricter than merely waiting for the queue to become empty. It still does not prove storage durability after power loss, database failover, partitions, every possible instruction-level race, long-term fairness/table growth, or external-effect safety. SIGKILL does not produce a returning zombie; the targeted expiry/reassignment test addresses that separate case.
