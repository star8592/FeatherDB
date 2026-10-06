# Repair / Forced-Loss Path v0

Status: executable research prototype, not frozen architecture.

## Purpose

Graceful rebalance assumes the source owner is still readable.

Forced-loss repair is different:

- the failed owner may be permanently unreadable;
- data must be copied from another surviving replica;
- restoring replication safety is more urgent than ordinary balancing;
- loss of every replica must be reported as unrecoverable, never hidden.

## Task semantics

A migration task now distinguishes:

    copy_source
    owner_to_replace
    target

For ordinary rebalance:

    copy_source == owner_to_replace

For forced repair:

    copy_source != owner_to_replace

The copy source is selected from surviving readable replicas of the same tablet.

Selection currently prefers:

    Active
      before
    Draining

Removed nodes are never considered readable copy sources.

## Repair priority

Tasks are ordered:

    Repair
      before
    Rebalance

A degraded tablet therefore consumes available migration budget before ordinary load-balancing work.

Repair still obeys the same:

- cluster-wide active-task limit;
- per-node active-task limit;
- cluster-wide byte budget;
- per-node byte budget;
- per-task byte budget;
- topology epoch fencing.

Urgency does not bypass resource safety.

## Repair cutover rule

Ordinary rebalance requires the candidate replica set to satisfy the configured placement policy.

Repair is different because the current set may already be degraded.

A repair cutover is allowed when:

1. the target is writable;
2. the resulting replica set has unique nodes;
3. the number of currently readable replicas strictly increases;
4. available zone diversity does not decrease;
5. available rack diversity does not decrease.

This permits stepwise recovery such as:

    [dead A, dead B, live C]
      -> [live D, dead B, live C]
      -> [live D, live E, live C]

without demanding that the first step already restore full RF.

## Unrecoverable loss

If a lost owner must be replaced but no surviving readable replica exists, scheduler creation fails with:

    NoRepairSource(tablet_id)

The simulator must never invent data from topology metadata.

Future production behavior should surface an explicit tablet/data-loss state and stop automatic claims of recovery.

## Relationship to topology authority

Failure suspicion alone must not execute this path.

The control plane must first make a durable decision that a node is removed/replaced for the current topology epoch.

Only then may the repair planner/scheduler treat the owner as lost.

This preserves the earlier rule:

    failure detection != durable topology authority

## Official-system alignment

ScyllaDB documents dead-node replacement as other live nodes streaming data to the replacement node. Its tablet-aware remove-node path rebuilds tablets onto new replicas before completing node removal.

References:
- https://docs.scylladb.com/manual/stable/operating-scylla/procedures/cluster-management/replace-dead-node.html
- https://docs.scylladb.com/manual/stable/operating-scylla/nodetool-commands/removenode.html
- https://docs.scylladb.com/manual/stable/troubleshooting/handling-node-failures.html

The same documentation also requires topology quorum for normal topology mutation. FeatherDB keeps that distinction: data repair can be urgent, but ownership authority remains a control-plane decision.

## Executable invariants

Current tests verify:

- Removed owner is never selected as copy source.
- A surviving replica is selected for Repair.
- Repair tasks sort ahead of Rebalance.
- One surviving replica can rebuild multiple lost owners sequentially.
- No surviving replica returns NoRepairSource.
- Repair cutover increases readable replica count.
- Repair does not reduce current zone/rack diversity.
- Existing migration budgets and epoch fencing still apply.

## Open work

1. Crash the copy source midway through Repair.
2. Crash/fill the target midway through Repair.
3. Re-select another surviving source without restarting the whole repair epoch.
4. Model a durable degraded-tablet state.
5. Separate temporarily unavailable from permanently removed.
6. Model repair debt and repair-age priority.
7. Add checksum/version validation before cutover.
8. Integrate anti-entropy once data-version semantics exist.


## Runtime health is not topology

The simulator now keeps transient node health outside durable topology membership.

Runtime health states:

    Healthy
    Suspect
    Unavailable

A node becoming Unavailable can make a tablet:

    Healthy -> Degraded -> Lost

without changing actual or desired ownership.

Only a durable topology transition to Removed authorizes forced-loss ownership replacement.

This preserves:

    failure detection != durable topology authority

and prevents a transient timeout from becoming an automatic destructive reshuffle.

## Repair source failover

If a Repair copy source becomes runtime-unavailable before copy completion:

1. the task is returned to Pending;
2. partial bytes are discarded conservatively;
3. another healthy surviving replica is selected;
4. the copy restarts from zero;
5. ownership remains unchanged until the restarted copy completes.

This is intentionally conservative until the storage layer can prove resumable verified chunk transfer.

## Durable truth and reconstructible work

Migration/Repair tasks are not currently treated as durable truth.

The durable state required to reconstruct work is:

    TopologyEpoch
    Actual Tablet Map
    Desired Tablet Map

After scheduler/process restart, unfinished work is regenerated from:

    actual -> desired

A test verifies that a Repair interrupted after partial copy is rebuilt from the durable maps and safely converges.

This keeps the control-plane state smaller and avoids a second persistent task journal unless future evidence proves it necessary.
