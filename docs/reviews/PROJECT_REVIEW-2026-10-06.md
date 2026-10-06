# FeatherDB Project Review — 2026-10-06

## Scope

This review covers the entire repository at commit `30b846c`: research notes, ADRs, architecture invariants, simulator specification, Rust workspace, tests, and the first placement experiment.

## Current state

FeatherDB is still correctly positioned as a **research/pre-prototype** project. The repository has a coherent hypothesis, but implementation coverage is far behind the written architecture.

- Documentation: ~1.3k lines, research-first and generally internally consistent.
- Rust implementation: one simulator crate, one placement implementation, one CLI scenario.
- Tests before this review: 3 unit tests.
- Accepted ADRs: 0.
- Proposed ADRs: hybrid control/data plane and tablet placement engine.
- Production network/storage/replication code: intentionally absent.

## What is strong

1. The project resists premature architecture freeze.
2. Safety, liveness, elasticity, resource, and operability invariants are already explicit.
3. Desired placement and physical migration are conceptually separated.
4. Failure detection is not confused with durable membership.
5. The low-resource target is treated as an architectural constraint, not an optimization pass.
6. The first real experiment exposed that movement and balance need a feasibility-aware model; raw movement percentage or raw weight share alone is not enough to judge a replicated placement algorithm.

## Gaps and risks

### P0 — repository truth mismatch

README says license is TBD, while Cargo metadata declared Apache-2.0. Until the project deliberately chooses and adds a license, Cargo metadata must not imply one.

### P0 — simulator/code mismatch

The simulator specification models logical time, health, migrations, actual vs desired replicas, churn, telemetry, and faults. Current code only computes a desired placement snapshot.

### P0 — strategy coupling

The current `Cluster::place()` mixes:
- candidate scoring,
- eligibility,
- failure-domain policy,
- replica selection.

This prevents fair algorithm comparisons and makes future policy testing difficult.

### P0 — correctness coverage

Only 3 tests exist against 21 pre-freeze invariants. The tests cover determinism, zone diversity, and draining-node exclusion only.

### P1 — weighting and fairness need a feasible-target model

The original `hash * weight` score was only a placeholder and has now been removed from the active simulator path. More importantly, with RF>1 a node cannot receive more than one replica of the same tablet, so raw capacity weight is not directly equal to replica-slot share. Movement must be decomposed into necessary movement and excess churn, and balance must be compared with a redundancy-constrained feasible target.

### P1 — no resource model yet

The 256–512 MiB target cannot be evaluated until the simulator accounts for:
- per-node metadata bytes,
- per-tablet metadata bytes,
- migration queue bounds,
- event queue bounds,
- planner working-set size.

### P1 — no topology state-machine implementation

The topology state machine and failure matrix exist only as documents. Epoch fencing, operation IDs, replay, and stale-incarnation handling have no executable model yet.

## Architectural conclusion

Do **not** start QUIC, storage, SWIM, Raft, or real replication yet.

The next correct step is to turn `feather-sim` into an executable architecture laboratory:

1. separate placement strategies from policy constraints;
2. add correct weighted rendezvous and hash-ring baselines;
3. add metrics and reproducible scenarios;
4. add desired/actual placement plus bounded migration;
5. then implement topology transactions and deterministic fault events.

Only after these layers falsify or support ADR-0001/0002 should FeatherDB enter the three-node prototype stage.

## Review verdict

The project direction is good, but the executable evidence is still too thin to accept either ADR. Continue **simulation-first** and treat every untested architecture statement as a hypothesis.
