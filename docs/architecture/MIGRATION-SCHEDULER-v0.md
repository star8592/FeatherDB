# Migration Scheduler v0

Status: research prototype, not frozen architecture.

## Purpose

The placement planner computes the desired tablet map. The migration scheduler is responsible for moving actual placement toward that desired map without pretending that planned ownership has already become physical reality.

The scheduler must protect foreground work, preserve replica safety, and reject stale topology work.

## State machine

Each replica movement is modeled as:

    Pending
      -> Copying
      -> ReadyToCutover
      -> Complete

A task can instead become Stale when its topology epoch no longer matches the current desired map.

Ownership does not change while a task is Pending or Copying. The old replica remains authoritative until the full tablet copy has completed and the cutover is safe.

## Budget dimensions

The current simulator models five independent limits:

1. maximum cluster-wide active migrations;
2. maximum active migrations touching one node;
3. maximum cluster-wide bytes copied per tick;
4. maximum bytes copied per node per tick;
5. maximum bytes copied by one task per tick.

Concurrency and throughput are intentionally separate controls.

The per-node byte budget was added after simulation showed that concurrency limits alone allowed a node to reuse completed slots multiple times in one tick and therefore exceed its intended throughput budget.

## Source and target states

Normal rebalance migration may read from:

- Active
- Draining

Normal rebalance may write only to:

- Active

Removed nodes cannot be used as a normal copy source.

This intentionally separates graceful drain from forced repair. A forced-loss path must reconstruct data from another surviving replica and will use Repair priority semantics rather than pretending the removed node is still readable.

## Epoch fencing

Each task carries the topology epoch that created it.

When desired placement changes:

1. unfinished work is cancelled;
2. the scheduler keeps current actual placement;
3. a new task set is generated from actual -> new desired;
4. old work cannot publish a cutover into the newer epoch.

The task vector is rebuilt instead of accumulating historical cancelled tasks, keeping scheduler state bounded under repeated desired-map changes.

## Cutover safety

A completed copy may cut over only when:

- the task epoch is current;
- the desired set still wants the target and no longer wants the source;
- the source still exists in actual ownership;
- the target is not already an owner;
- the resulting replica set has no duplicate nodes;
- configured zone/rack diversity remains valid.

A replica set is canonicalized by sorting NodeId values. This prevents semantically identical sets with different ordering from producing false topology changes or false non-convergence.

## Current limitation

Some future desired-map transitions may require more than one ownership change to be committed atomically or in a carefully chosen order.

The current scheduler can leave a fully copied task in ReadyToCutover when a single replacement would temporarily violate placement safety.

This is deliberate: safety wins over convergence.

A later simulator campaign must determine whether we need:

- dependency ordering between migration tasks;
- augmenting-path scheduling;
- temporary extra replicas;
- atomic multi-replica cutover.

## Official-system alignment

TiKV PD explicitly limits concurrent scheduling tasks and also provides store-level scheduling rate limits because scheduling consumes CPU, memory, network, and I/O resources and can affect online traffic:

https://tikv.org/docs/7.1/deploy/configure/pd-configuration-file/
https://tikv.org/docs/5.1/reference/architecture/scheduling/
https://tikv.org/docs/3.0/tasks/configure/limit/

ScyllaDB tablets are migrated in the background by the load balancer while service remains available:

https://docs.scylladb.com/manual/stable/architecture/tablets.html

FeatherDB borrows the separation of planning from bounded execution, not the full TiKV or Scylla implementation.

## Acceptance tests already executable

- ownership does not change before copy completion;
- cluster-wide and per-node concurrency limits are enforced;
- per-node byte throughput cannot exceed its tick budget;
- desired-epoch changes cancel old work and rebuild from actual state;
- Removed source cannot start normal rebalance;
- planner desired placement converges through the scheduler;
- canonical replica ordering prevents false map inequality.

## Next tests

- source crash while Copying;
- target crash while Copying;
- ReadyToCutover task blocked by another tablet transition;
- repair priority preempts normal rebalance;
- foreground-pressure signal dynamically lowers budgets;
- disk-full target pauses/cancels copy;
- 100-node churn repeatedly changes desired placement without unbounded task growth;
- deterministic replay of every migration transition boundary.
