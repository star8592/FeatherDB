# Leaderless Data Semantics v0

Status: executable deterministic model; production replication transport/storage wiring remains open.

## Goal

Model the minimum leaderless data semantics FeatherDB needs before building `featherd`:

    N / R / W quorum
    causality
    concurrent-write conflicts
    deletes/tombstones
    bounded hinted handoff
    anti-entropy convergence

This is deliberately not a general distributed transaction system.

## Quorum policy

A policy contains:

    replication_factor = N
    read acknowledgements = R
    write acknowledgements = W

`R + W > N` guarantees read/write replica-set intersection for the same replica set. It does not by itself create linearizability, serialize concurrent writers, or make failed writes disappear.

The executable model therefore preserves an important failure semantic: a write that fails to reach W may already have been accepted by one or more replicas and can later be observed or repaired.

## Version model

Each mutation carries:

    VersionVector causal context
    HlcTimestamp
    origin NodeId
    value or tombstone

The version vector is authoritative for causal dominance.

HLC provides a compact monotonic timestamp and deterministic optional tie-break/order signal. It is not used to erase causally concurrent updates by default.

### HLC safety property

The local clock remains monotonic when physical time moves backward. Observing a remote HLC advances the local logical component according to the maximum observed physical/logical timestamp.

## Conflict policy

Default:

    KeepSiblings

If two versions are causally concurrent, both remain visible to the caller.

A client that reads both siblings receives their merged causal context. A subsequent write using that context causally dominates both predecessors and reduces the set back to one version.

Optional research policy:

    LastWriterWinsHlc

This is deterministic but explicitly not the default because timestamp ordering does not turn concurrent business updates into a semantically correct merge.

## Deletes

Delete is represented as a causally versioned tombstone, not immediate physical absence.

This prevents a delayed old write or delayed hint from resurrecting a value when the tombstone causally dominates it.

Physical tombstone garbage collection is not yet modeled. Production GC will require an anti-entropy/grace invariant before deletion of causal history is safe.

## Consistency modes represented

### Eventual

The model exposes one-response read/write operations. They maximize availability but may observe stale replicas.

### Quorum

Reads require R responses and writes require W acknowledgements. With `R + W > N`, acknowledged writes intersect subsequent quorum reads for the same replica set.

### Key-scoped causal

A causal read receives a required VersionVector token and succeeds only when an online replica's key context dominates or equals the token.

This is deliberately a key-scoped causal primitive, not yet a claim of cluster-wide causal+ consistency across arbitrary keys or transactions.

## Hinted handoff

When a target replica is offline, the coordinator may store the exact original mutation as a hint.

Hints are:

- bounded by an explicit maximum;
- replayed without generating a new version;
- idempotent under version-vector merge;
- best effort only.

When the hint budget is exhausted, hints are dropped and a counter records the loss.

This follows the engineering lesson from Cassandra: hints shorten inconsistency windows but cannot be the sole convergence mechanism.

## Anti-entropy

`anti_entropy_key()` merges the non-dominated sibling set from available replicas and reapplies it to those replicas.

The first model is key-scoped rather than Merkle-tree based. This isolates version/conflict correctness before adding a range-hash acceleration structure.

A test intentionally sets hint capacity to zero, misses a replica write, restores the node, and proves anti-entropy converges the missing value.

Future range-level anti-entropy may use Merkle/range hashes, but it must preserve exactly the same version semantics.

## Executable invariants

Tests currently prove:

1. quorum bounds are validated;
2. `R + W > N` intersection is observable;
3. failed quorum writes may leave partial mutations;
4. HLC survives physical-clock rollback monotonically;
5. concurrent writes remain siblings;
6. a causal overwrite dominates all siblings it observed;
7. a tombstone prevents old hinted data from resurrecting a value;
8. hint queues are bounded and dropped hints are counted;
9. anti-entropy converges a replica even when no hint exists;
10. causal reads refuse to go backward relative to a required context;
11. HLC-LWW is deterministic but opt-in.

## CAS boundary

A normal leaderless quorum write does not provide a linearizable compare-and-set primitive.

FeatherDB must not expose a fake CAS that allows two concurrent coordinators to both succeed against the same expected value.

Before Phase 7 exposes CAS, one of these must be chosen and proved:

- a small per-key/per-tablet consensus/LWT path;
- a compare-and-set mode explicitly documented as non-linearizable/conditional-best-effort (not preferred);
- omit CAS from the first public prototype until the strong path exists.

The current recommendation is to keep PUT/GET/DELETE leaderless and require an explicit strong path for CAS.

## Next work

1. range-hash/Merkle acceleration for anti-entropy;
2. bounded repair scheduling integrated with the existing Repair priority executor;
3. persist real mutations through the selected storage engine;
4. route mutation/read messages over the production transport API;
5. multi-key/session causal semantics only if product requirements justify the metadata cost;
6. explicit strong CAS design before exposing CAS publicly.
