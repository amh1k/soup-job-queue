# Tasks 5–6 verification

Implemented chaos and load executables, an independent audit, process supervision, isolated schemas, evidence collection, and the delivery documents. Runtime evidence remains incomplete.

## Executed checks

- `make build vet`: passed for all five executables.
- `go test -race -count=1 -v ./...`: executable tests passed; real PostgreSQL integration tests skipped. Final passing output is in `results/checks/tests-corrected.log`; earlier outputs are also retained.
- Audit corruption fixtures detected missing/duplicate effects, stale effect versions, unexpected/missing jobs, ownership/terminal-state violations, wrong payloads, and inconsistent acknowledged cancellations. Valid failure/cancellation fixtures passed.
- Load tests checked the fixed profile ratios, repeatable manifests, percentile calculation, generator drops, send-rate validation, and backlog growth detection.
- A real child-process supervisor test killed a sleep process, replaced it, and reaped all children. It did not run database jobs.
- Actual independent-audit and monitor SQL passed PostgreSQL 18.6 single-user checks. The raw SQL output is in `results/checks/sql-smoke.log`; this does not verify scanning/concurrent transactions.
- Compose YAML parsed with four normal workers and two opt-in experiment services. Container image/startup behavior remains unverified.
- Three chaos and two load CLI invocations returned exit code 1 at database preflight. Every manifest, metadata file, event stream, and summary is retained. `results/checks/invocations.json` contains the commands, process exit codes, stdout, and stderr.

A final build initially failed because a newly referenced metrics field was absent. The correction passed build/vet and the full unit/race suite. Both failed logs and corrected logs are linked from `evidence.md`; no workload result was changed. Nineteen real-database integration tests skipped in the final check.

## Remaining required runtime work

Run the full real-database integration suite, then three full 10,000-job/eight-worker chaos runs with different seeds, and both 100 RPS profiles. Inspect every failed/incomplete run, preserve its raw output, and fix the cause before repeating. Collect deployment resource measurements, verify Docker startup and persistence, and update the verdict based on actual results. The present failures are environmental, and provide no evidence that the job protocol passed or failed under chaos.

The independent audit has an additional real-database corruption test in `internal/jobs/audit_integration_test.go`; it is included but unrun here.
