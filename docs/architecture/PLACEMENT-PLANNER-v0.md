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

## Candidate planner implementation

The first planner prototype should be a simple deterministic greedy allocator, not a complex optimizer:

1. start from current placement;
2. calculate normalized load vs feasible target;
3. identify the most overloaded source and underloaded eligible target;
4. consider tablets whose move preserves/improves failure-domain constraints;
5. use WRH ranking as deterministic tie-break;
6. apply one virtual move;
7. repeat until within tolerance or movement budget is exhausted.

This is intentionally stateful.

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
