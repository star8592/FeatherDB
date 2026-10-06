# Placement Baseline Experiment — 2026-10-06

## Purpose

Turn ADR-0002 from intuition into measurable evidence by comparing:
- weighted virtual-node hash ring;
- mathematically weighted rendezvous hashing (WRH);
- WRH plus hierarchical failure-domain selection.

This is a simulator experiment, not a production benchmark.

## Quality gate

Before collecting results:

- `cargo test`: **10 passed, 0 failed**
- `cargo clippy --all-targets --all-features -- -D warnings`: **pass**
- `cargo fmt --check`: **pass**

## WRH formula

The simulator uses the weighted HRW form equivalent to:

`score = -weight / ln(U)`

and selects the highest score, where `U` is a deterministic hash mapped into the open interval (0, 1).

The current implementation uses `f64` deliberately for research. Production placement must later define cross-platform deterministic numeric semantics before independent planners are allowed to recompute ownership.

## Scenario A — heterogeneous strong-node join

Before:
- RF=2
- weights 1:2:4
- one node per zone

After:
- add node weight=8 in a new zone

10,000 tablets:

| Strategy | Movement ratio | Excess join bytes | Zone collisions | Replica counts |
|---|---:|---:|---:|---|
| Hash ring | 0.418500 | 0 | 0 | 1:1850, 2:3311, 3:6469, 4:8370 |
| WRH | 0.421700 | 0 | 0 | 1:1790, 2:3499, 3:6277, 4:8434 |
| Constrained WRH | 0.421700 | 0 | 0 | 1:1790, 2:3499, 3:6277, 4:8434 |

### Interpretation

The ~42% movement is not automatically bad. The joining node has weight 8 while the previous cluster totals only 7, so large transfer is expected.

More importantly, `excess_join_bytes = 0` for all three strategies: every newly created replica in this join scenario lands on the joining node. The experiment therefore shows **minimal-disruption behavior for this topology**, not 42% arbitrary churn.

Raw node weight cannot be compared directly to replica-slot percentage when RF>1. A node may hold at most one replica of a tablet, and failure-domain constraints further restrict the feasible allocation. Future balance metrics must compare against a feasible constrained target.

## Scenario B — failure-domain pressure

Before:
- RF=3
- 6 equal-weight nodes
- 2 nodes in each of zones A/B/C

After:
- add one equal-weight node in new zone D

10,000 tablets:

| Strategy | Movement ratio | Excess join bytes | Zone collisions | New node replicas |
|---|---:|---:|---:|---:|
| Hash ring | 0.129600 | 0 | 4,829 | 3,888 |
| WRH | 0.145000 | 0 | 4,254 | 4,350 |
| Constrained WRH | 0.183767 | 0 | **0** | 5,513 |

### Interpretation

The constrained policy eliminates all avoidable same-zone replica collisions, but it moves more data because the new failure domain becomes useful for redundancy.

Again, extra movement between unchanged nodes is zero. The higher movement is policy-driven movement to the joining node.

This establishes an important design distinction:

**minimal churn does not mean minimum bytes at any cost.**  
The optimizer must first satisfy safety/failure-domain constraints, then minimize movement within that feasible set.

## Release-binary scaling snapshot

Single process, Scenario A + strategy comparison:

| Tablets | Elapsed | Peak RSS |
|---:|---:|---:|
| 1,000 | <0.01 s | ~3.2 MiB |
| 10,000 | 0.01 s | ~4.9 MiB |
| 100,000 | 0.13 s | ~24.5 MiB |
| 1,000,000 | 1.37 s | ~220.6 MiB |

### Memory conclusion

Runtime is already fast enough for experimentation, but the current simulator representation scales roughly with tablet count because it materializes tablets and multiple placement maps.

This is **not** a production-memory measurement, but it creates a concrete simulator engineering task:
- add a compact/streaming representation for million-scale campaigns;
- separately model production metadata bytes per tablet/node;
- do not confuse simulator object overhead with the FeatherDB daemon budget.

## New questions created by this experiment

1. What is the correct feasible capacity target under RF and failure-domain constraints?
2. Should placement optimize aggregate zone capacity before ranking nodes inside the zone?
3. How should zone/rack capacity be weighted when domain sizes differ?
4. Can we measure excess churn for leave, weight change, and domain-policy changes?
5. What deterministic integer/fixed-point formulation should replace `f64` if planners recompute placement independently?
6. How many tablets can a 256 MiB node actually afford in the production metadata model?

## Decision

ADR-0002 remains **Proposed**.

Evidence supports:
- tablet indirection;
- explicit failure-domain policy;
- WRH as a serious candidate;
- separating safety constraints from movement minimization.

Evidence is not yet sufficient to freeze:
- final weighting semantics;
- domain hierarchy semantics;
- tablet count;
- production numeric representation.


## Iteration 2 — feasible capacity target

A capacity-only feasible inclusion target was added:

    p_i = min(1, lambda * weight_i)
    sum(p_i) = RF

This captures a fundamental replication constraint: a node cannot hold two replicas of the same tablet.

At 100,000 tablets:

### Heterogeneous strong-node join

| Strategy | Max feasible-capacity inclusion error |
|---|---:|
| Hash ring | 0.163960 |
| WRH | 0.153800 |
| Constrained WRH | 0.153800 |

WRH improves on the ring but still misses the capacity-only optimum materially. With weights 1:2:4:8 and RF=2, the capped proportional target makes the weight-8 node eligible for essentially one replica of every tablet; stateless top-k WRH only places it on about 84.6% of tablets.

This is evidence that **weighted rendezvous should remain a candidate-ranking primitive, not be assumed to be the final tablet allocator**.

### Failure-domain pressure

| Strategy | Capacity-only error | Zone collisions |
|---|---:|---:|
| Hash ring | 0.043141 | 49,242 |
| WRH | 0.003831 | 42,810 |
| Constrained WRH | 0.116909 | 0 |

The constrained result is not a failure of the constraint policy. The capacity-only target does not model zone diversity, so once domain constraints bind, it is the wrong objective.

The next metric must therefore be **domain-aware feasible capacity**, not raw node capacity.

## Architecture consequence

FeatherDB should distinguish:

1. **Candidate ranking** — deterministic, low-churn ordering such as WRH.
2. **Placement planner** — stateful optimizer over current assignments, node/domain capacities, tablet sizes/hotness, and safety constraints.
3. **Migration scheduler** — bounded execution from actual placement toward desired placement.

This three-layer split is now the leading architecture hypothesis.

ScyllaDB independently provides useful engineering evidence for this direction: its tablet load balancer uses actual tablet/disk utilization to decide migrations rather than relying only on stateless hashing:
https://docs.scylladb.com/manual/stable/architecture/tablets.html
