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


## Runtime source-failover lab

Scenario:

- 100 tablets
- RF=3
- owner 1 durably Removed
- owners 2 and 3 initially Healthy
- node 4 is repair target
- all Repair tasks initially choose node 2 as copy source
- repair starts, then node 2 becomes runtime Unavailable

Observed:

    before-source-loss:
      started=2
      copied=8 MiB
      active=2
      tablet availability=Degraded 2/3

    after source loss:
      source_failovers=100
      copied=8 MiB
      active=2
      tablet availability=Degraded 1/3

    after repair:
      converged=true
      ticks=101
      completed=100
      tablet availability=Degraded 2/3

    after node 2 returns:
      ownership unchanged
      tablet availability=Healthy 3/3

Interpretation:

Transient runtime health changes do not rewrite topology ownership. Repair can change copy source independently of owner identity.

## Scheduler restart experiment

A Repair was interrupted after 40% of its bytes were copied.

The scheduler process was then reconstructed only from:

- topology epoch;
- actual placement;
- desired placement.

The reconstructed task restarted from full bytes and converged to desired placement.

Result: pass.

This supports keeping migration tasks reconstructible instead of making them a separate durable source of truth.

## Updated quality gate

- cargo test: **31 passed, 0 failed**
- cargo clippy --all-targets --all-features -- -D warnings: **pass**
- cargo fmt --check: **pass**
