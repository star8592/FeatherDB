# Physical Range Resize Experiment — 2026-10-07

## Goal

Turn tablet split/merge from a count-only controller into an executable hash-range metadata transformation.

## Model

Hash space:

    [0, 2^64)

Every tablet owns one contiguous half-open range and a canonical replica set.

The committed map must have:

- no gaps;
- no overlaps;
- unique tablet IDs;
- canonical unique replica IDs;
- next_tablet_id greater than every live tablet ID.

## Growth/shrink lab

Initial state:

    tablet count = 1
    generation = 0
    bytes = 10,000
    replicas = [1,2,3]

Six coordinated split generations:

    count = 64
    generation = 6
    bytes = 10,000

Six coordinated merge generations:

    count = 1
    generation = 12
    bytes = 10,000
    range = [0, 2^64)
    replicas = [1,2,3]

All sampled tokens remained routable.

## Safety tests

The executable suite verifies:

- split preserves full hash-space coverage;
- split preserves total bytes;
- split inherits replica ownership;
- repeated splits introduce no gaps;
- merge after split restores one contiguous range;
- merge rejects adjacent ranges with different replica sets;
- tampered range boundaries are rejected;
- duplicate replica IDs are rejected;
- stale topology epochs cannot commit;
- stale generations cannot overwrite newer maps;
- replay after successful commit is idempotent;
- lifecycle count/generation/cooldown and physical range map commit together;
- physical merge failure leaves lifecycle state unchanged.

## Quality gate

- cargo test: **55 passed, 0 failed**
- cargo clippy --all-targets --all-features -- -D warnings: **pass**
- cargo fmt --check: **pass**

## Conclusion

Tablet lifecycle now has two executable layers:

    resize policy
      -> coordinated lifecycle plan
      -> physical range-map transformation

The next resize problem is no longer basic metadata correctness. It is choosing production split boundaries and integrating resize with migration/fault scheduling.
