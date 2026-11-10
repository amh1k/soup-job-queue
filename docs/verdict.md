# DON'T SHIP at 100 requests/second

There is currently insufficient runtime evidence to ship. All three chaos invocations and both load invocations failed before their workloads started because this sandbox denies PostgreSQL sockets. The implementation has passing unit tests, SQL smoke checks, and unexecuted real-database tests. None of those establishes capacity or correctness through the required 10,000-job runs.

| Evidence | Actual result |
| --- | --- |
| Chaos seeds 1, 2, 3; 10,000 planned jobs each; eight workers | Three failed preflight attempts, zero completed workload runs, zero submitted jobs, zero worker kills |
| 100 RPS mixed profile; 30s warm-up and 5m measurement | Preflight failed; no latency or throughput measurements |
| 100 unique creates/second; same timings | Preflight failed; no latency or throughput measurements |
| Unit tests with Go race detector | Passed; database integration cases skipped |
| Real process supervisor test | Passed: SIGKILL, replacement, and child reaping using a sleep process; this was not a database worker chaos run |
| SQL smoke checks | Passed in PostgreSQL single-user mode; no multi-connection concurrency evidence |

The first action is to run the existing integration suite and at least three full chaos seeds on an accessible PostgreSQL instance, preserving every result and fixing any invariant violations. Then run both default load profiles on the intended hardware with the intended worker count. The current raw failures and exact rerun commands are in `evidence.md` and the README.

Capacity also needs a concrete deployment decision. With a 100 ms average execution duration, four default workers have an ideal ceiling near 40 job completions/second, and eight experiment workers near 80, before database and scheduling overhead. One hundred unique creates/second exceeds those ideal limits. The mixed profile schedules 50% POSTs with 10% of those replaying keys, approximately 45 unique jobs/second, plus reads/lists/cancellations. Do not assume cancellation will remove enough work to make the four-worker deployment sustainable. Increase worker capacity based on measured execution times and headroom, then rerun; twelve to sixteen workers is a hypothesis to measure, not a demonstrated fix.

The load driver uses project-selected acceptance targets: no invariant violations or unexpected HTTP/transport errors; no dropped scheduled arrivals; dispatch RPS within 5% of target; p95 response latency below 250 ms; p95 scheduling lag below 25 ms; at least two queue metrics; backlog slope at most 0.1 queued jobs/second and final queued count no more than ten above the initial count. These thresholds were not specified by the assignment and have not been achieved in a measured run here. Passing them would support this declared workload and hardware, rather than all production conditions.

Resource evidence includes available cgroup/host limits, API pool acquisition totals/empty acquisitions, queue age/backlog/attempts, combined API+harness Go heap/goroutines, and worker CPU/max-RSS at exit. Database CPU/memory and independent production-process measurements still need external collection. The in-process API uses the actual HTTP handler/store, but it shares resources with the generator and does not validate container packaging or production topology.
