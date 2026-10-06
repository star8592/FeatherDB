# Migration Budget Experiment — 2026-10-06

## Goal

Verify that planner output can converge through a bounded migration scheduler and that node-local throughput limits materially control convergence speed.

## Scenario

- 1,000 tablets
- 64 MiB per tablet
- RF=2
- 3 equal nodes -> add a fourth equal node
- quota-aware planner produces 500 replica moves
- total copied data: 32,000 MiB
- global copy budget: 32 MiB/tick
- max active tasks: 4
- max bytes per task: 8 MiB/tick

Two node-local byte budgets were compared.

## Results

| Profile | Per-node byte budget | Ticks | Converged | Bytes copied | Max active |
|---|---:|---:|---|---:|---:|
| node-rate-16m | 16 MiB/tick | 2,000 | yes | 32,000 MiB | 4 |
| node-rate-32m | 32 MiB/tick | 1,000 | yes | 32,000 MiB | 4 |

Ticks are deterministic simulator scheduling quanta, not wall-clock seconds.

## Interpretation

With every new replica targeting the joining node, that node is the bottleneck.

At 16 MiB/tick, the target needs 2,000 ticks to receive 32,000 MiB.

At 32 MiB/tick, the global 32 MiB/tick budget becomes the bottleneck and convergence takes 1,000 ticks.

The experiment caught an earlier modeling flaw: concurrency limits alone did not bound per-node bytes because a completed task could free a slot and another task could consume the same node during the same tick.

The scheduler now has both concurrency and byte-rate budgets.

## Quality gate

- cargo test: 23 passed, 0 failed
- cargo clippy --all-targets --all-features -- -D warnings: pass
- cargo fmt --check: pass

## Decision

Keep separate controls for:

- global concurrency;
- per-node concurrency;
- global bytes/tick;
- per-node bytes/tick;
- per-task bytes/tick.

Do not collapse these into one generic rebalance-speed knob.
