# Grouped Tablet Cutover Experiment — 2026-10-07

## Goal

Verify that a safe final replica set can be reached when every one-at-a-time ownership replacement would temporarily violate failure-domain policy.

## Scenario

RF=3.

Current ownership:

    node 1 / zone A
    node 4 / zone B
    node 3 / zone C

Desired ownership:

    node 5 / zone A
    node 2 / zone B
    node 3 / zone C

Task pairing by deterministic node ordering yields:

    1(A) -> 2(B)
    4(B) -> 5(A)

Individually:

    1(A) -> 2(B)
      would produce B/B/C

and:

    4(B) -> 5(A)
      would produce A/A/C

Both are rejected as unsafe intermediate cutovers.

## Grouped behavior

Both data copies complete first.

The scheduler then evaluates the whole candidate set:

    [2(B), 3(C), 5(A)]

It:

- equals the committed desired replica set;
- has unique nodes;
- preserves three distinct zones.

The scheduler atomically commits the complete tablet replica set.

## Result

- completed tasks: 2
- grouped cutovers: 1
- final actual == desired
- converged: true
- no unsafe intermediate ownership state was committed

## Quality gate

- cargo test: **32 passed, 0 failed**
- cargo clippy --all-targets --all-features -- -D warnings: **pass**
- cargo fmt --check: **pass**

## Conclusion

A general task dependency graph is not yet justified.

Use this escalation order:

1. safe individual cutover;
2. safe grouped final-set cutover for one tablet;
3. only add dependency graphs / temporary extra replicas if simulator scenarios still deadlock.

This follows FeatherDB's complexity-minimization principle.
