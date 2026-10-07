# Leaderless Data Semantics Experiment — 2026-10-08

## Scope

Deterministic in-memory model for Phase 5:

- N/R/W quorum;
- HLC;
- version-vector causality;
- sibling conflict preservation;
- causal overwrite;
- versioned tombstones;
- bounded hinted handoff;
- key-level anti-entropy;
- key-scoped causal reads.

## Baseline policy

    N = 3
    R = 2
    W = 2

Therefore `R + W > N`.

## Results

Ten focused tests pass.

Important observed behaviors:

### Quorum intersection

With one replica down, a W=2 write succeeds. A later R=2 read using a different availability pattern still intersects the acknowledged write set and returns the value.

### Failed write ambiguity

With only one of three replicas available, W=2 fails. The available replica still stores the mutation, and an eventual R=1 read may observe it.

This prevents the API from promising that a timeout means "nothing happened".

### Concurrent siblings

Two coordinators writing from independent causal contexts produce concurrent version vectors. Anti-entropy preserves both as siblings rather than choosing a wall-clock winner.

### Causal resolution

Reading the siblings and writing with the merged read context creates a new version that dominates both predecessors.

### Tombstone versus delayed hint

A replica misses both a value and its later delete. Replaying the old value hint followed by the tombstone hint converges to the tombstone only; the old value cannot resurrect.

### Hint loss + anti-entropy

With hint capacity set to zero, an unavailable replica misses a successful quorum write. After it returns, key-level anti-entropy repairs it successfully.

### Causal read

A read carrying a required causal context returns `CausalNotReady` while only a stale replica is reachable. After the exact mutation reaches that replica, the causal read succeeds.

## Interpretation

The model validates the minimum semantics needed for a leaderless FeatherDB data plane without claiming linearizable writes or transactions.

Hints are useful latency/convergence optimization, not authority. Anti-entropy remains mandatory.

CAS is intentionally not implemented because a normal quorum write cannot safely provide linearizable compare-and-set under concurrent coordinators.
