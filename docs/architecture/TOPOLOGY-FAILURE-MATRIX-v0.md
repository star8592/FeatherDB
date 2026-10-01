# Topology Failure Matrix v0

Purpose: adversarial scenarios for deterministic simulation before production implementation.

| Scenario | Injected failure | Required result |
|---|---|---|
| Join | crash before registration commit | no durable member appears |
| Join | crash after Joining commit | retry resumes or aborts same op safely |
| Join | source dies during streaming | choose another valid source; bounded retry |
| Join | target disk full | no ownership commit; cluster remains valid |
| Join | network partition isolates target | target cannot self-promote |
| Leave | crash before replacement replicas exist | old ownership remains committed |
| Leave | crash after copy but before commit | copied data is harmless staging data |
| Leave | crash after ownership commit | retry cleanup; never roll ownership backward |
| Replace | old node returns while replacement joins | old incarnation fenced |
| Replace | replacement crashes repeatedly | operation remains resumable/cancellable |
| Failure detector | overloaded healthy node looks dead | suspect only; no automatic destructive removal |
| Metadata | leader changes during topology transaction | committed prefix preserved; operation resumes |
| Metadata | metadata quorum lost | topology mutations stop; existing data plane continues where consistency policy permits |
| Events | duplicate/remove event storm | notifications coalesced and queues bounded |
| Resource | many concurrent joins | admission control bounds memory/network/disk usage |
| Resource | 1 slow disk among heterogeneous nodes | scheduler reduces work assigned to slow node; no cluster-wide stall |
| Epoch | stale streaming worker finishes late | cannot publish stale placement |
| Repair | partition heals with divergent replicas | anti-entropy converges according to version/conflict policy |
| Restart | wiped data dir with same IP | treated as new incarnation, never trusted as old replica |
| Churn | join/leave loop | metadata and tombstones remain bounded/GC-able |

## Simulator event vocabulary

`Crash(node)`, `Restart(node)`, `Partition(a,b)`, `Heal(a,b)`, `Delay(link,duration)`, `Drop(link,p)`, `DiskFull(node)`, `DiskCorrupt(node,range)`, `ClockSkew(node,delta)`, `CpuStall(node,duration)`, `Join(node)`, `Leave(node)`, `Replace(old,new)`, `Resize(node,capacity)`.

## Initial deterministic campaigns

1. 3 nodes, RF=3, one join while one source crashes.
2. 5 nodes, RF=3, simultaneous leave + join + partition.
3. 3 nodes, metadata leader crash at every transition boundary.
4. 10 heterogeneous nodes, repeated capacity changes and 30% packet loss.
5. 100 simulated nodes, churn with bounded topology/event queues.

Each campaign must run from a seed and emit a replayable event trace.
