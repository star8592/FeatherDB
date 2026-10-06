# Forced-Loss Repair Experiment — 2026-10-07

## Goal

Verify that loss of an owner does not require reading from the dead node and that replication can be restored from surviving replicas under the same bounded migration scheduler.

## Executable cases

### One lost owner

Actual:

    [dead 1, live 2, live 3]

Desired:

    [live 2, live 3, live 4]

Expected:

- task priority = Repair;
- owner_to_replace = 1;
- copy_source is 2 or 3;
- node 1 is never read;
- after copy/cutover, actual == desired.

Result: pass.

### Two lost owners, one surviving replica

Actual:

    [dead 1, dead 2, live 3]

Desired:

    [live 3, live 4, live 5]

Expected:

- two Repair tasks;
- both may stream from live replica 3;
- repairs proceed sequentially under a one-task budget;
- final RF is restored.

Result: pass.

### All replicas lost

Actual:

    [dead 1, dead 2]

Desired:

    [live 3, live 4]

There is no surviving copy source.

Expected:

    NoRepairSource(tablet)

Result: pass.

### Repair vs Rebalance

When both task classes are queued and only one slot is available:

    Repair starts first.
    Rebalance remains Pending.

Result: pass.

## Quality gate

- cargo test: **27 passed, 0 failed**
- cargo clippy --all-targets --all-features -- -D warnings: **pass**
- cargo fmt --check: **pass**

## Architectural conclusion

Normal migration and forced-loss repair can share the same bounded copy executor, but they cannot share the same source/ownership semantics.

The task model must preserve three identities:

    copy_source
    owner_to_replace
    target

This distinction is now part of the FeatherDB migration architecture.
