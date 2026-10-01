# User Pain Evidence

> Status: research input, not architecture freeze.

This document turns public operational complaints and issue patterns into testable FeatherDB constraints. Individual reports are evidence of failure modes, not proof that every user experiences them.

## 1. Operational complexity

### FoundationDB
Community operators have described FoundationDB as powerful but difficult to build and operate for smaller teams, with substantial platform-engineering burden. Current issue trackers also show ongoing work around memory allocation, crash recovery, backup behavior, observability, and upgrades.

**Derived hypothesis:** FeatherDB should make the single-binary, small-cluster path a first-class product, not a demo path.

**Candidate acceptance tests:**
- New 3-node cluster from clean machines in < 60 seconds after binaries exist.
- No external coordinator, service discovery system, JVM, Python runtime, ZooKeeper, etcd, or Kubernetes required.
- Upgrade procedure can be explained in one page and exercised continuously in simulation.

## 2. Scale-down and topology state machines

Operator issue trackers for distributed databases show real-world cases where scale-down/decommission can become stuck or interact badly with concurrent scale-up/replacement.

**Derived constraint:** topology transitions must be explicit, idempotent, resumable state machines. Join/leave/replace must survive coordinator crash and repeated commands.

**Candidate invariant:** a topology operation is either safely committed or safely retryable; no node is permanently half-joined or half-removed.

## 3. Metadata amplification

Large distributed systems repeatedly expose bugs where metadata, descriptors, parsing structures, or buffers amplify memory far beyond the user payload.

**Derived constraint:** no tablet-local operation may require O(total_tablets) resident metadata. Metadata APIs need pagination/streaming, bounded caches, memory accounting, and backpressure.

## 4. Tablet/shard overhead

Fine-grained tablets improve movement and balancing, but each tablet has non-zero CPU/RAM/background-work cost.

**Derived constraint:** tablet granularity must be adaptive and budget-aware. Tablet count is a resource decision, not merely a logical partitioning decision.

## 5. Hot partitions and noisy neighbors

Operational reports show that one hot/large partition can create queueing and timeout symptoms concentrated on a node.

**Derived constraint:** scheduler and admission control must understand load, not only bytes. Placement weight should eventually include disk, CPU, latency and hotness signals.

## 6. Storage failure loops

Storage engines can enter pathological retry loops under persistent flush/write failures.

**Derived constraint:** every retry loop must have explicit bounded backoff, failure classification, observability, and simulation coverage.

## 7. Product principle extracted from pain research

FeatherDB should aim to make common operations boring:

- bootstrap
- add node
- remove node
- replace dead node
- rebalance
- repair
- backup/restore
- rolling upgrade
- diagnose degraded cluster

The architecture is not accepted merely because it is theoretically correct. It must minimize the amount of external automation and tribal knowledge required to keep a small cluster healthy.

## Research rule

For every pain item we add:

1. Preserve source URL/title/date in the research notes.
2. Separate observed report from inferred root cause.
3. Convert the root-cause hypothesis into an invariant or benchmark where possible.
4. Attempt to reproduce the failure class in deterministic simulation.
