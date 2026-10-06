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


## Iteration 3 — leave, weight change, and zone-aware target

The simulator now measures:
- topology-transition changes unrelated to the explicitly affected node set;
- a first zone-aware feasible capacity target;
- leave and weight-change scenarios.

Quality gate:
- cargo test: 13 passed, 0 failed
- clippy with -D warnings: pass

At 100,000 tablets:

### Heterogeneous leave

Weights before: 1:2:4:8, RF=2. Node weight=2 leaves.

| Strategy | Movement ratio | Unrelated changed tablets | Capacity error |
|---|---:|---:|---:|
| Hash ring | 0.162740 | 0 | 0.053420 |
| WRH | 0.172175 | 0 | 0.060540 |
| Constrained WRH | 0.172175 | 0 | 0.060540 |

### Heterogeneous weight change

Weights change from 1:2:4:8 to 1:6:4:8, RF=2.

| Strategy | Movement ratio | Unrelated changed tablets | Capacity error |
|---|---:|---:|---:|
| Hash ring | 0.167010 | 0 | 0.097025 |
| WRH | 0.151770 | 0 | 0.092915 |
| Constrained WRH | 0.151770 | 0 | 0.092915 |

### Failure-domain pressure

Equal-weight nodes, RF=3, three zones with two nodes each; a seventh equal-weight node joins as a fourth zone.

| Strategy | Movement ratio | Unrelated changed tablets | Zone collisions | Zone-aware capacity error |
|---|---:|---:|---:|---:|
| Hash ring | 0.128477 | 0 | 49,242 | 0.043141 |
| WRH | 0.143547 | 0 | 42,810 | 0.003831 |
| Constrained WRH | 0.181827 | 0 | 0 | 0.116909 |

### Interpretation

1. The ranking algorithms preserve strong minimal-disruption behavior for these isolated topology changes: unrelated remapping is zero.
2. Plain WRH balances equal-weight nodes very closely, but does not enforce failure-domain diversity.
3. Constrained WRH removes all avoidable zone collisions, but overuses the single-node new zone relative to the capacity-balanced domain-aware target.
4. Therefore a stateful planner has a concrete optimization opportunity: keep the zero-collision property while moving the single-node zone toward its feasible target share.
5. The current zone-aware target is only a first-level zone model. Rack hierarchy, unequal tablet bytes, hotness, and hard disk-pressure admission remain unmodeled.

This strengthens the current architecture split:

    deterministic ranking
        -> stateful constraint/capacity planner
        -> desired placement
        -> bounded migration


## Iteration 4 — stateful quota planner

A stateful desired-map planner has now been implemented.

The first one-move-at-a-time greedy version was rejected because it could stop in a local optimum:
- strong join: one replica short of the exact target;
- node removal: stale ownership could remain even though a globally valid solution existed.

The replacement planner uses:
- exact integer node quotas;
- zone-first quotas when zone diversity is feasible;
- current-placement stickiness;
- WRH deterministic tie-breaking;
- quota-preserving pair-swap repair.

### Quality gate

- `cargo test`: **17 passed, 0 failed**
- `cargo clippy --all-targets --all-features -- -D warnings`: **pass**
- `cargo fmt --check`: **pass**

### 10K planner results

| Scenario | Movement ratio | Moves | Count lower bound | Gap | Zone collisions | Target error |
|---|---:|---:|---:|---:|---:|---:|
| strong heterogeneous join | 0.500000 | 10,000 | 10,000 | 0 | 0 | 0.000043 |
| heterogeneous leave | 0.196550 | 3,931 | 3,499 | 432 | 0 | 0.000000 |
| weight 2 -> 6 | 0.144350 | 2,887 | 2,817 | 70 | 0 | 0.000053 |
| fourth failure domain joins | 0.142833 | 4,285 | 4,285 | 0 | 0 | 0.000071 |

The count lower bound ignores per-tablet uniqueness and failure-domain constraints, so a positive gap is not automatically avoidable churn.

### 100K scale run

The release binary ran the complete four-scenario comparison at 100,000 tablets in:

- elapsed: **1.20 s**
- peak RSS: **~35.6 MiB**

Stateful planner results:

| Scenario | Moves | Converged | Zone collisions | Target error |
|---|---:|---|---:|---:|
| strong heterogeneous join | 100,000 | yes | 0 | 0.000004 |
| heterogeneous leave | 38,746 before final lower-bound instrumentation run; exact target achieved | yes | 0 | 0.000000 |
| weight change | 29,028 | yes | 0 | 0.000005 |
| fourth failure domain | 42,857 | yes | 0 | 0.000009 |

The simulator remains lightweight enough for 100K-tablet research. Million-tablet compact representation is still required before churn/fault campaigns.

### Architecture conclusion

The experiment now supports a stronger split:

    WRH candidate ranking
        -> quota/constraint-aware desired-map planner
        -> committed desired tablet map
        -> bounded migration scheduler
        -> actual tablet map

WRH remains useful, but the desired map is no longer assumed to be the direct output of a stateless hash function.

ADR-0002 remains Proposed because:
- node-removal movement optimality is not yet understood;
- rack-aware target feasibility is still incomplete;
- unequal tablet bytes/hotness are not modeled;
- split/merge lifecycle is not executable yet.


## Iteration 5 — constraint-aware removal lower bound

The previous movement lower bound was too weak for forced removal because it ignored the fact that a missing replica cannot be assigned to a node/failure domain already represented by that tablet.

For the 10K heterogeneous leave scenario:

- removed-node replica slots: **3,499**
- target counts after removal: node 1 = 2,000; node 3 = 8,000; node 4 = 10,000
- retained counts: node 1 = 1,790; node 3 = 6,277; node 4 = 8,434
- node 4 therefore needs **1,566** new replicas
- among the 3,499 affected tablets, **2,365 already contain node 4**
- only `3,499 - 2,365 = 1,134` missing slots can directly accept node 4
- unavoidable extra movement: `1,566 - 1,134 = 432`

Therefore the true constrained lower bound is:

    3,499 + 432 = 3,931 moves

The current stateful planner performs exactly **3,931 moves**, so the apparent 432-move gap was measurement error, not planner churn.

### 100K confirmation

| Metric | Result |
|---|---:|
| planner moves | 38,746 |
| constrained movement lower bound | 38,746 |
| movement gap | **0** |
| zone collisions | 0 |
| target error | 0.000000 |
| full four-scenario release run | 1.18 s |
| peak RSS | ~35.8 MiB |

### Decision update

For the tested RF=2 single-node removal topology, movement optimality is now understood and the planner reaches the constrained lower bound.

This does **not** prove global optimality for:

- RF>2;
- multiple simultaneous removed owners;
- multiple nodes per failure domain;
- unequal tablet byte sizes;
- hotness-aware placement.

The simulator should extend the lower-bound proof only when those scenarios become relevant instead of introducing a general min-cost-flow planner prematurely.
