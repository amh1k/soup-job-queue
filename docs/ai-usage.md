# AI usage disclosure

## How I used AI

I used OpenAI Codex to help interpret the assignment, explore the design, and understand concurrency concepts such as leases, claim versions, and transaction boundaries. Codex generated substantial parts of the implementation: the API, PostgreSQL store, migrations, worker runtime, Docker configuration, tests, chaos/load harnesses, and independent audit. It also helped write the README, diagrams, delivery documents, and the reflection draft.

## Decisions I accepted or challenged

I chose PostgreSQL as the queue after discussing the trade-offs of adding Redis. I accepted the design using row locks, leases, claim versions, and a transaction that commits the database effect and successful job state together. I asked for explanations of those mechanisms and their limits throughout implementation; the resulting design is documented in [design.md](design.md).

I challenged the inclusion of large generated manifests in Git because they bloated the repository without adding useful source code. They were removed from tracking and ignored. The harness recreates planned inputs from its seed and settings, while original raw results are preserved in compressed archives. I also requested clearer architecture diagrams and documentation organized around the assignment's deliverables.

## What I verified

I ran three batches of three 10,000-job chaos seeds and both 100 RPS load profiles through Docker Compose. Codex helped inspect the saved reports. All nine chaos runs passed their final audits. The mixed load passed, but the create-only load failed because eight workers could not keep up with incoming jobs. Its final audit passed after the queue drained. I retained that failure, and it supports the DON'T SHIP verdict.

I then ran `make verify-deployment` locally. The full Go test suite passed against PostgreSQL with race detection and zero skips, including long-running heartbeat renewal and a paused worker resuming after reassignment. The deployment readiness, execution, restart, and persistence checks also passed. Codex separately ran unit/race tests, build/vet checks, and SQL smoke checks where its environment allowed them.

## Corrections and remaining limits

A later Codex audit identified gaps in its generated harness: cancellation errors were ignored, and evidence-writing failures could leave misleading success reporting. Codex corrected those issues and added regression checks, executable/source identification, and a repeatable archive command. The earlier experiment results were preserved rather than rewritten. The final full chaos/load batch used the corrected harness with executable fingerprints and source checksums matching the current source. Sufficient capacity for 100 unique creates per second has not been demonstrated.

The [evidence index](evidence.md) records successful runs, failed attempts, and remaining limitations. The reflection remains an AI-assisted draft for my review before submission.

The second experiment batch still lacked the executable/source fingerprints added to the corrected image. Those results are preserved, but a rebuilt-image rerun remains necessary to verify the harness corrections.

I reran all three chaos seeds and both load profiles with `--build`. Codex confirmed the new metadata fingerprints and source checksums, inspected the final audits, and updated the documents. The corrected-harness chaos runs and mixed profile passed; create-only again failed backlog stability while passing its final audit. Earlier unverified-image runs remain preserved as historical evidence.
