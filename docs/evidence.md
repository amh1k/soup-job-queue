# Evidence index

## Latest verified corrected-harness runs — 2026-11-11, 17:30–17:48 UTC

All five runs contain executable hashes and a build source manifest. Every recorded source checksum matches the current corresponding source file. Worker-start events also identify child executable hashes. This supersedes the earlier requirement to rerun the corrected harness; historical runs and failures below remain unchanged.

| Run | Outcome | Results | Summary | Raw archive |
| --- | --- | --- | --- | --- |
| Chaos seed 1 | PASS | 14 kills; 8267 succeeded / 161 failed / 1572 cancelled; audit passed | [summary](../results/chaos-20261111T173028Z-seed1-4203661562/summary.json) | [archive](../results/archives/chaos-20261111T173028Z-seed1-4203661562.tar.gz) |
| Chaos seed 2 | PASS | 15 kills; 7826 succeeded / 165 failed / 2009 cancelled; audit passed | [summary](../results/chaos-20261111T173155Z-seed2-1105843608/summary.json) | [archive](../results/archives/chaos-20261111T173155Z-seed2-1105843608.tar.gz) |
| Chaos seed 3 | PASS | 15 kills; 7897 succeeded / 150 failed / 1953 cancelled; audit passed | [summary](../results/chaos-20261111T173313Z-seed3-2887195233/summary.json) | [archive](../results/archives/chaos-20261111T173313Z-seed3-2887195233.tar.gz) |
| Load mixed | PASS | 100 RPS; p95 9.91 ms; 44.92 job completions/s; audit passed | [summary](../results/load-20261111T173432Z-seed1-1407469591/summary.json) | [archive](../results/archives/load-20261111T173432Z-seed1-1407469591.tar.gz) |
| Load create | FAIL: backlog growth | 100 RPS; p95 16.12 ms; 68.37 job completions/s; audit passed | [summary](../results/load-20261111T174011Z-seed1-2817258392/summary.json) | [archive](../results/archives/load-20261111T174011Z-seed1-2817258392.tar.gz) |

Each corrected chaos run accepted all 10,000 jobs and reported zero audit violations, zero unexpected worker exits, and zero attempt-limit exhaustions. There were **44 deliberate worker kills** across these three runs. The failed job counts are configured execution failures, not harness failures.

| Latest load metric | Mixed | Create only |
| --- | --- | --- |
| Measured requests | 30,000 | 30,000 |
| Successful HTTP RPS | 100 | 100 |
| Errors / drops | 0 / 0 | 0 / 0 |
| Response p95 / p99 | 9.91 / 65.84 ms | 16.12 / 56.47 ms |
| Job completions/second | 44.92 | 68.37 |
| First / last backlog | 0 / 2 | 1,100 / 10,559 |
| Backlog growth/second | 0.009 | 31.61 |
| Final audit | Passed | Passed |

Mixed found all 14,882 jobs: 14,871 succeeded and 11 cancelled. Create-only eventually completed all 33,032 jobs after draining; its capacity failure remains a failure. Across all batches: **nine completed chaos runs, 128 planned kills, three passing mixed-load runs, three create-only capacity failures, and five earlier preflight failures** (20 workload CLI invocations total). Deployment verification attempts are recorded separately below.


## Initial Docker experiments — 2026-11-11

The user ran these experiments locally through Docker Compose; Codex inspected their saved reports. All used eight child worker processes and isolated PostgreSQL schemas. These are five completed workloads: three chaos passes, one mixed-load pass, and one create-load capacity failure. Earlier failed preflight attempts remain recorded below.

| Run | Outcome | Observations | Summary | Raw archive |
| --- | --- | --- | --- | --- |
| Chaos seed 1 | PASS | 10,000 accepted jobs; 14 kills; audit passed | [summary](../results/chaos-20261111T141852Z-seed1-1370551346/summary.json) | [archive](../results/archives/chaos-20261111T141852Z-seed1-1370551346.tar.gz) |
| Chaos seed 2 | PASS | 10,000 accepted jobs; 14 kills; audit passed | [summary](../results/chaos-20261111T142009Z-seed2-3485737030/summary.json) | [archive](../results/archives/chaos-20261111T142009Z-seed2-3485737030.tar.gz) |
| Chaos seed 3 | PASS | 10,000 accepted jobs; 14 kills; audit passed | [summary](../results/chaos-20261111T142124Z-seed3-929485685/summary.json) | [archive](../results/archives/chaos-20261111T142124Z-seed3-929485685.tar.gz) |
| Load mixed | PASS | 100 RPS; p95 8.03 ms; 0 errors; audit passed | [summary](../results/load-20261111T142406Z-seed1-2415849045/summary.json) | [archive](../results/archives/load-20261111T142406Z-seed1-2415849045.tar.gz) |
| Load create | FAIL: backlog growth | 100 RPS; p95 11.32 ms; 0 errors; audit passed | [summary](../results/load-20261111T142940Z-seed1-2012199788/summary.json) | [archive](../results/archives/load-20261111T142940Z-seed1-2012199788.tar.gz) |

Each chaos run found all 10,000 expected jobs, with no audit violations and no unexpected worker exits:

| Seed | Succeeded | Failed | Cancelled | Attempts |
| --- | --- | --- | --- | --- |
| 1 | 8002 | 155 | 1843 | 8175 |
| 2 | 7878 | 165 | 1957 | 8061 |
| 3 | 8002 | 149 | 1849 | 8167 |

Failed jobs in these chaos runs were the configured execution failures; no jobs exhausted their attempt limits. Audit success means no lost keys/payloads, no residual ownership/nonterminal jobs, matching success effects and versions, zero failed/cancelled effects, and consistent acknowledged cancellations. It is evidence for these schedules, not proof of every possible race.

## Commands and load measurements

```sh
docker compose --profile experiments run --rm chaos -seed 1
docker compose --profile experiments run --rm chaos -seed 2
docker compose --profile experiments run --rm chaos -seed 3
docker compose --profile experiments run --rm load -mode mixed
docker compose --profile experiments run --rm load -mode create
```

Both load runs used 30 seconds of warm-up and five minutes of measurement at 100 scheduled RPS. Warm-up traffic affects the database but is excluded from latency/throughput statistics. Each measured cohort contained 30,000 requests.

| Metric | Mixed | Create only |
| --- | --- | --- |
| Successful request RPS | 100 | 100 |
| HTTP errors / drops | 0 / 0 | 0 / 0 |
| Response p95 | 8.03 ms | 11.32 ms |
| Response p99 | 16.49 ms | 32.08 ms |
| Job completions/second | 44.99 | 69.63 |
| First / last measured backlog | 2 / 2 | 903 / 9,988 |
| Backlog slope/second | -0.001 | +30.48 |
| Final audit | Passed | Passed |

The mixed run found all 14,882 expected jobs: 14,871 succeeded and 11 cancelled. The create run eventually completed all 33,032 expected jobs after draining; its capacity failure is retained even though final correctness checks passed.

## Raw evidence packaging and limits

The archives preserve original event streams, API/worker logs, metadata, summaries, audits, HTTP counts, tracked diffs, and load reports, where present. Generated input manifests and operation plans are excluded from version control and archives; the committed generator, recorded seed, and options recreate planned inputs, while concurrent runtime timing varies. Original local run directories remain intact. Use `make evidence` (Python 3.11+) to package subsequent runs and verify existing archives without overwriting them. [Archive checksums](../results/archives/sha256.json) identify the submitted archive bytes. Extract with `tar -xzf results/archives/<run>.tar.gz` into a separate directory if preserving local originals.

Docker metadata reports Linux/amd64, Go 1.26.8, eight visible CPUs, about 7.7 GiB host-reported RAM, and no explicit CPU/memory cgroup limit in the experiment container. This is shared-environment information, not measured database CPU/memory. The experiment API shares a process with the generator. The Docker image lacks Git/source files: metadata commit and source hashes are empty, so these runs cannot independently establish exact image-to-commit provenance. No 16-worker rerun is recorded.

## Earlier failed attempts — 2026-11-10

Three chaos invocations and two load invocations failed PostgreSQL preflight with `socket: operation not permitted`. They submitted zero jobs and killed zero workers. These failures were not overwritten or relabelled.

| Attempt | Summary |
| --- | --- |
| chaos-20261110T164411Z-seed1-2871885838 | [summary](../results/chaos-20261110T164411Z-seed1-2871885838/summary.json) |
| chaos-20261110T164411Z-seed2-1903454023 | [summary](../results/chaos-20261110T164411Z-seed2-1903454023/summary.json) |
| chaos-20261110T164411Z-seed3-3183409780 | [summary](../results/chaos-20261110T164411Z-seed3-3183409780/summary.json) |
| load-20261110T164411Z-seed1-1597510854 | [summary](../results/load-20261110T164411Z-seed1-1597510854/summary.json) |
| load-20261110T164411Z-seed1-1783461771 | [summary](../results/load-20261110T164411Z-seed1-1783461771/summary.json) |

Exact earlier exit codes/stdout/stderr: [invocations](../results/checks/invocations.json).

## Development checks and gaps

Earlier sandbox-only [Go race-test output](../results/checks/rerun-20261111T140501Z/tests.log) passed, with nineteen PostgreSQL integration tests skipped. [Resumed vet status](../results/checks/rerun-20261111T140501Z/vet-resumed-status.json) passed. The 2026-11-10 later rerun was interrupted and is not counted as a completed check.

Earlier [corrected build/vet](../results/checks/build-vet-corrected.log), [unit/race checks](../results/checks/tests-corrected.log), and [single-user SQL smoke checks](../results/checks/sql-smoke.log) passed. The preceding compilation failures remain in [build log](../results/checks/build-vet-final.log) and [test log](../results/checks/tests-final-v2.log).

Still unverified: normal four-worker deployment capacity at 100 RPS, dedicated production topology/resource measurements, a real network interruption during commit, database failover/power loss/clock jumps, and capacity improvements beyond the measured eight-worker limit. This index consolidates execution status and limitations for the submission.

## Harness corrections after these measurements

The chaos cancellation loop now fails unexpected cancellation responses while tolerating phase shutdown. Final success reporting now follows raw evidence sync/close; JSON files are written atomically. New images carry source checksums, metadata identifies the harness executable, and worker-start events identify the worker executable. `make evidence` makes raw-output packaging repeatable. These changes do not retroactively alter or reclassify the recorded workload runs; the latest fingerprinted runs above verify this corrected harness under PostgreSQL.

Two additional database lifecycle tests now cover persisted heartbeat renewal across lease periods and a SIGSTOP/SIGCONT worker returning after reassignment. `make verify-deployment` runs the full PostgreSQL suite and checks the real Compose deployment in a disposable project. The user subsequently executed these checks successfully in the Docker run below; the earlier sandbox skips remain historical records. Existing load capacity results and the verdict are unchanged.

The first isolated deployment verification attempt is retained in [its summary](../results/checks/soup-verify-c29a749558c6-20261111T163255Z/summary.json) and [command log](../results/checks/soup-verify-c29a749558c6-20261111T163255Z/commands.json). Compose configuration validation passed, but startup and cleanup commands could not access the Docker daemon. This is a failed preflight attempt, not an executed integration/deployment check.

## Passed PostgreSQL and deployment verification — 2026-11-11

The user ran `make verify-deployment`. [Summary](../results/checks/soup-verify-c3f2feee479f-20261111T163344Z/summary.json), [all commands and exit codes](../results/checks/soup-verify-c3f2feee479f-20261111T163344Z/commands.json), and [full PostgreSQL race-test output](../results/checks/soup-verify-c3f2feee479f-20261111T163344Z/003.log) are retained. Every command returned zero, and the test output contains zero skips and no failed tests.

This run exercised the targeted database suite, including concurrent claims, stale-claim fencing, cancellation races, effect rollback, corruption detection, and SIGKILL recovery. The new long-running heartbeat test passed in 3.78 seconds; the paused/resumed child-process test passed in 4.61 seconds.

The ordinary four-worker Compose deployment became ready, executed a persistence job, stopped with `down` while retaining its volume, and started again. The API returned an identical persisted job before and after restart, and PostgreSQL still contained exactly one effect. Cleanup of the disposable project's containers and volume also succeeded. No capacity measurement was performed by this verification command. The earlier permission-denied attempt above remains a failed preflight record.

## Additional runs — 2026-11-11, 16:58–17:24 UTC

These five additional workloads completed, but their metadata has neither `executable_sha256` nor `build_source_manifest`. The corrected harness always records the executable hash. These results therefore do not establish execution of the corrected image; they are retained as additional older-image measurements. Rebuild or use `docker compose run --build` before repeating.

| Run | Result | Summary | Raw archive |
| --- | --- | --- | --- |
| chaos-20261111T165801Z-seed1-3475236253 | PASS; final audit passed | [summary](../results/chaos-20261111T165801Z-seed1-3475236253/summary.json) | [archive](../results/archives/chaos-20261111T165801Z-seed1-3475236253.tar.gz) |
| chaos-20261111T165915Z-seed2-2342791297 | PASS; final audit passed | [summary](../results/chaos-20261111T165915Z-seed2-2342791297/summary.json) | [archive](../results/archives/chaos-20261111T165915Z-seed2-2342791297.tar.gz) |
| chaos-20261111T170033Z-seed3-3968383568 | PASS; final audit passed | [summary](../results/chaos-20261111T170033Z-seed3-3968383568/summary.json) | [archive](../results/archives/chaos-20261111T170033Z-seed3-3968383568.tar.gz) |
| load-20261111T171052Z-seed1-302806140 | PASS; final audit passed | [summary](../results/load-20261111T171052Z-seed1-302806140/summary.json) | [archive](../results/archives/load-20261111T171052Z-seed1-302806140.tar.gz) |
| load-20261111T171627Z-seed1-1125020389 | FAIL: backlog growth; final audit passed | [summary](../results/load-20261111T171627Z-seed1-1125020389/summary.json) | [archive](../results/archives/load-20261111T171627Z-seed1-1125020389.tar.gz) |

Each additional chaos seed accepted all 10,000 jobs, killed 14 workers, and reported no audit violations. Mixed load had response p95 9.38 ms and stable backlog. Create-only had response p95 17.49 ms, approximately 68.35 execution completions/second, and backlog growth from 965 to 10,430. It eventually completed all 33,032 jobs with a passing final audit. Across both measurement batches there are now six completed chaos runs (84 kills total), two passing mixed-load runs, and two create-only capacity failures, plus the original five preflight failures. The earlier detailed tables retain their original batch measurements.
