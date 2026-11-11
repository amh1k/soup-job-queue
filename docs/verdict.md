# DON'T SHIP at an unrestricted 100 requests/second

The corrected harness passed three full 10,000-job/eight-worker chaos runs with 44 deliberate process kills and zero final audit violations. Their executable fingerprints are recorded and source checksums match the current source. Earlier runs are preserved separately. The mixed 100 RPS profile also passed. However, eight workers could not sustainably execute 100 unique creations/second: the measured completion rate was 68.37 jobs/second and the queued backlog rose from 1,100 to 10,559 during measurement, approximately 31.61 jobs/second. Its final audit passed after the queue drained; that does not turn the capacity failure into a pass. See [raw evidence and exact results](evidence.md).

In the corrected-harness measurement batch, the API accepted all 30,000 measured requests in each profile without errors or drops. Response p95 was 9.91 ms for mixed and 16.12 ms for create only. Fast enqueue responses do not establish acceptable job completion delay or sustainable execution capacity. The mixed profile introduces approximately 45 unique jobs/second; it is a narrower workload than 100 unique creates/second.

The ordinary Compose deployment has four workers, while experiments used eight. These measurements therefore do not establish capacity for the default deployment even under the passing mixed profile. With 100 ms mean simulated execution, eight workers have an ideal ceiling around 80 executions/second before overhead; observed create-run throughput was lower.

## Required before changing the verdict

1. Define the intended 100 RPS workload and execution-latency budget. The corrected chaos/load harness and the complete PostgreSQL race-test suite are now verified; further work is about sustainable capacity and production conditions, with results in the evidence index.
2. Increase execution capacity and measure again. Sixteen workers is a proposed experiment, not an implemented or validated fix. Retain the eight-worker failure and measure database contention, backlog, and end-to-end job latency as worker count increases.
3. Match the deployment worker count to measured capacity with headroom; define whether 100 RPS means mixed traffic or unique job submissions. Normal Compose startup, readiness, worker execution, and restart/persistence have now passed in an isolated deployment; capacity for the four-worker deployment remains unmeasured.
4. Collect dedicated API, worker, and database CPU/memory/pool measurements. The current API and generator share a process, host resources may be shared, and exact Git commit IDs are unavailable in Docker. Latest runs do identify executable hashes and matching build source checksums.

The load driver uses project-selected targets: no audit violations/errors/drops, dispatch RPS within 5%, response p95 below 250 ms, scheduling lag p95 below 25 ms, and stable backlog (slope at most 0.1 jobs/second and last count no more than ten above first). The assignment does not specify these numerical thresholds. Mixed passed them; create failed backlog stability. No failed result has been discarded.

The demonstrated guarantee covers committed PostgreSQL effects for this simulated executor. Database failover, partitions, storage power loss, clock jumps, sustained table growth, and external side effects require additional evidence or protocols. A DON'T SHIP conclusion is a completed delivery decision grounded in measured limits; passing every performance scenario is not a prerequisite to reporting it honestly.
