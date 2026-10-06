# Placement Planner v0

Status: research hypothesis, not frozen architecture.

## Why this layer exists

A hash function answers "which nodes rank highly for this tablet?" It does not, by itself, solve the full FeatherDB objective:

- heterogeneous storage capacity;
- replication-factor feasibility;
- rack/zone separation;
- current assignment stickiness;
- unequal tablet sizes;
- hot tablets;
- disk pressure;
- bounded movement.

Because FeatherDB already treats the tablet map as committed control-plane state, final placement does not need to be a pure stateless hash function.

## Proposed three-layer model

### 1. Candidate ranking

Deterministic ordering, initially WRH.

Properties:
- cheap to recompute;
- minimal disruption when candidate set changes;
- useful deterministic tie-breaker;
- not authoritative ownership by itself.

### 2. Placement planner

Inputs:
- current committed tablet map;
- tablet sizes and optional heat;
- node capacities and pressure state;
- failure-domain topology;
- replication policy;
- candidate rankings;
- topology epoch.

Hard constraints:
1. one replica per node per tablet;
2. RF where enough eligible nodes exist;
3. required zone/rack diversity where feasible;
4. no new ownership on draining/removed nodes;
5. no placement above hard resource admission limits.

Optimization objectives, in order:
1. satisfy safety constraints;
2. reduce maximum normalized resource utilization;
3. approach feasible domain-aware capacity targets;
4. minimize moved bytes/tablets;
5. avoid hotspot concentration;
6. preserve deterministic tie-breaking.

### 3. Migration scheduler

The planner may compute a desired map immediately. The scheduler controls physical convergence with:
- per-node byte budgets;
- per-disk concurrency;
- cluster-wide migration limits;
- foreground latency protection;
- repair-before-balance priority.

## Capacity target

For the node-only case, the simulator now uses capped proportional inclusion:

    p_i = min(1, lambda * weight_i)
    sum(p_i) = RF

This is only the first feasibility model. Domain-aware capacity requires a hierarchical constraint solution.

## Important consequence

"Minimal movement" is subordinate to safety and feasible balance.

A new zone may justify more movement because it materially improves failure-domain redundancy. We measure **excess churn** separately from policy-required movement.

## Current planner prototype

The first single-move greedy prototype was deliberately discarded after simulation showed that it could get trapped in a locally valid but globally incomplete state during node removal and highly skewed joins.

The active research prototype now separates the problem into:

1. calculate exact integer replica-count targets from feasible capacity;
2. if strict zone diversity is feasible, allocate zone quotas first;
3. allocate node quotas inside each chosen zone;
4. prefer current replicas whenever quota feasibility allows;
5. use WRH only as deterministic ranking/tie-breaking;
6. run quota-preserving pair swaps to recover additional stickiness without changing final capacity or failure-domain guarantees.

This produces a **desired map**. It does not perform physical migration.

At 100,000 tablets, the prototype converges in all current join/leave/weight/domain scenarios with zero avoidable zone collisions and near-zero target error.

### Movement optimality metric

The planner records:

    count_movement_lower_bound
    actual_changed_replicas
    movement_gap = actual - lower_bound

The lower bound is intentionally weak: it only compares current node replica counts with target node replica counts and ignores per-tablet uniqueness and failure-domain constraints.

Therefore:

- gap = 0 is strong evidence of minimum movement for that scenario;
- gap > 0 is a research signal, not proof of waste;
- an exact minimum may require a constrained matching/min-cost-flow or augmenting-path model.

Current 10K results:

| Scenario | Moves | Count lower bound | Gap |
|---|---:|---:|---:|
| strong heterogeneous join | 10,000 | 10,000 | 0 |
| heterogeneous leave | 3,931 | 3,499 | 432 |
| weight 2 -> 6 | 2,887 | 2,817 | 70 |
| new fourth failure domain | 4,285 | 4,285 | 0 |

The node-removal gap is the next important planner optimization target.

### Planner vs migration scheduler

Do not reintroduce a move-rate budget into the planner.

The planner answers:

    What should the committed desired tablet map be?

The migration scheduler answers:

    How quickly and safely can actual placement approach it?

This separation is now enforced architecturally.

## Research questions

- Can greedy planning converge without oscillation?
- What is the correct domain-aware capacity target?
- Should disk bytes and request heat be separate objective dimensions?
- How much state must the planner hold for 1M+ tablets?
- Can planning be streamed or partitioned to avoid O(global tablets) transient memory?
- How is planner determinism guaranteed across architecture/compiler versions?
- When should tablets split/merge instead of move?

## External engineering reference

ScyllaDB's current tablet load balancer uses actual tablet/disk utilization and issues migrations to equalize load, reinforcing the distinction between logical tablet mapping and a stateful balancing process:

https://docs.scylladb.com/manual/stable/architecture/tablets.html
