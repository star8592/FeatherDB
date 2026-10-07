# Durable RuntimeCoordinator Resize Experiment — 2026-10-07

## Goal

Wire PREPARED/CURRENT topology snapshots into the real TabletRuntimeCoordinator resize path and verify crash recovery at every durability boundary.

## Execution order

    current runtime snapshot
      -> exact target snapshot preview
      -> PREPARED + Sync
      -> RuntimeCoordinator commit_resize
      -> target snapshot equality check
      -> CURRENT + Sync

Migration task queues remain reconstructible.

## Focused integration tests

Seven durable-runtime tests are now executable:

1. normal prepare -> apply -> publish;
2. crash after PREPARED before apply;
3. DiskFull during prepare;
4. crash after runtime apply / CURRENT Put before Sync;
5. DiskFull during CURRENT publication;
6. recovered target reconstructs post-resize migration work;
7. crash at every state-machine edge.

## Crash matrix result

| Crash state | Recovered durable generation | Recovery mode |
|---|---:|---|
| PreparePutPending | 0 | Current(old) |
| PrepareSyncPending | 0 | Current(old) |
| Prepared | 1 | ReplayPrepared |
| PublishPutPending | 1 | ReplayPrepared |
| PublishSyncPending | 1 | ReplayPrepared |
| Complete | 1 | Current(new) |

Each ReplayPrepared case is then published to CURRENT and a second recovery observes only the new stable current snapshot.

## Important invariant

RuntimeCoordinator is never allowed to mutate the range/catalog generation before PREPARED has crossed Sync. Conversely, once PREPARED is durable, recovery has complete target replay material even if process death occurs before or during CURRENT publication.

## Remaining work

- use the same transaction layer for topology-epoch membership changes, not only resize;
- serialize all topology transactions through the future control-plane consensus path;
- garbage-collect obsolete PREPARED records safely;
- attach V3 disk/network faults directly to durable resize campaign seeds;
- benchmark real Fjall/redb implementations behind DurableStore semantics.
