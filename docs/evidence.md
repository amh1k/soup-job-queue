# Evidence index

Every CLI invocation below is preserved. A preflight attempt is not a completed workload run.

| Kind / profile | Seed | Workload started | Exit / outcome | Raw summary |
| --- | --- | --- | --- | --- |
| chaos | 1 | False | 1 / failed DB preflight | [summary](../results/chaos-20261110T164411Z-seed1-2871885838/summary.json) |
| chaos | 2 | False | 1 / failed DB preflight | [summary](../results/chaos-20261110T164411Z-seed2-1903454023/summary.json) |
| chaos | 3 | False | 1 / failed DB preflight | [summary](../results/chaos-20261110T164411Z-seed3-3183409780/summary.json) |
| load / mixed | 1 | False | 1 / failed DB preflight | [summary](../results/load-20261110T164411Z-seed1-1597510854/summary.json) |
| load / create | 1 | False | 1 / failed DB preflight | [summary](../results/load-20261110T164411Z-seed1-1783461771/summary.json) |

Actual counts: **3 failed chaos preflight attempts; 0 completed chaos workloads; 2 failed load preflight attempts; 0 completed load measurements.** No jobs were submitted and no database workers were started.

The common error was `socket: operation not permitted` connecting to local PostgreSQL. The raw stdout/stderr and command exit codes are in [invocations.json](../results/checks/invocations.json). Each summary directory also contains its saved manifest, source/config metadata, tracked diff, and raw event stream; load attempts include the scheduled operation plan. No failed run was removed or converted into a pass.

Other checks: [initial unit/race output](../results/checks/tests.log), [final unit/race output](../results/checks/tests-final.log), [build/vet output](../results/checks/build-vet.log), [SQL smoke output](../results/checks/sql-smoke.log), [static checks](../results/checks/static-checks.log).

Future successful runs should be added here alongside these failures. A directory without a final summary is an interrupted/incomplete invocation and must also be retained. Fresh database schemas are deliberately retained for diagnosis; their names are recorded in run events/summary once creation succeeds.


A final metrics change initially failed compilation because the new pool-acquisition field was missing. That failure is retained in [build-vet-final.log](../results/checks/build-vet-final.log) and [tests-final-v2.log](../results/checks/tests-final-v2.log). The field was added, and the corrected build/vet and full unit/race checks passed: [build-vet-corrected.log](../results/checks/build-vet-corrected.log), [tests-corrected.log](../results/checks/tests-corrected.log). Nineteen real-PostgreSQL integration tests still skipped. An additional [settings-query smoke check](../results/checks/sql-settings-check.log) passed in single-user mode. These development checks are separate from the five failed CLI workload attempts above.
