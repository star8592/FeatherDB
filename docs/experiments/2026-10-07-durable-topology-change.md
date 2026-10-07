# Durable Topology-Epoch Change Experiment — 2026-10-07

## Goal

Use the same replayable PREPARED/CURRENT protocol for node topology changes, without allowing SWIM failure detection to directly mutate ownership authority.

## FTS2 replay material

The durable snapshot now includes replication factor and the complete node map (NodeId, weight, zone, rack, AdminState) in addition to the tablet lifecycle. Recovery no longer accepts a caller-supplied Cluster.

## Tests

Five focused topology-change tests pass:

1. full cluster topology snapshot round-trip;
2. replacing cluster topology preserves exact range lifecycle;
3. normal durable epoch change prepares, applies and publishes;
4. crash after PREPARED/before topology apply recovers the new cluster and reconstructs migration;
5. equal/older epoch rejection, DiskFull-before-prepare fencing, and SWIM-Dead evidence/authority separation are covered by executable tests.

## Authority test

A three-node SWIM cluster is run until node 3 is globally Dead. RuntimeCoordinator remains:

    TopologyEpoch = 7
    node 3 AdminState = Active

Then an explicit proposed Cluster at epoch 8 marks node 3 Removed. Only after PREPARED is durable does the runtime apply epoch 8. The resulting ownership Repair is derived from the durable topology change.

## Recovery result

When a new strong node is introduced at epoch 8 and the process crashes after PREPARED but before apply, recovery reconstructs:

    epoch 8
    the new node map
    the previous durable actual tablet replicas
    a new desired placement

The recovered migration scheduler is initially unconverged and subsequently converges without a persisted task queue.
