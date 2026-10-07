#![forbid(unsafe_code)]

pub mod compact;
pub mod compact_catalog;
pub mod compact_migration;
pub mod compact_window;
pub mod data_semantics;
pub mod disk;
pub mod durable_runtime;
pub mod fault;
mod hash;
pub mod membership;
pub mod message_replay;
pub mod metrics;
pub mod migration;
pub mod model;
pub mod placement;
pub mod planner;
pub mod range_resize;
pub mod resize;
pub mod reverse_index;
pub mod runtime_coordinator;
pub mod split_boundary;
pub mod topology_snapshot;
pub mod transport;

pub use compact::{CompactPlacement, CompactPlacementError};
pub use compact_catalog::{CompactCatalogError, CompactTabletCatalog};
pub use compact_migration::{CompactMigrationCursor, CompactMigrationMove};
pub use compact_window::{
    CompactWindowError, CompactWindowPhase, CompactWindowReconcileReport, CompactWindowScheduler,
};

pub use data_semantics::{
    CausalRelation, ConflictPolicy, DataError, HlcClock, HlcTimestamp, LeaderlessDataCluster,
    QuorumPolicy, QuorumPolicyError, ReadOutcome, ResolvedValue, SiblingSet, VersionVector,
    VersionedValue, WriteOutcome, resolve_siblings,
};

pub use disk::{
    ControlCommitState, ControlRecord, DirectMemoryStore, DiskCompletion, DiskError, DiskOpId,
    DiskPoll, DiskRequest, DiskSubmit, DurableControlWriter, DurableStore, SimDisk,
    read_control_record,
};

pub use durable_runtime::{
    DurableResizeError, DurableResizeProgress, DurableResizeTransaction,
    DurableTopologyChangeError, DurableTopologyChangeTransaction, RecoveredTabletRuntime,
    recover_tablet_runtime,
};

pub use fault::{
    FaultAction, FaultEvent, FaultReplayReport, FaultTrace, FaultTraceParseError,
    apply_disk_fault_action, apply_network_fault_action, replay_migration_faults,
    replay_migration_faults_with_network,
};

pub use membership::{
    MemberState, MemberStatus, MemberUpdate, MembershipCluster, MembershipConfig,
    MembershipConfigError, MembershipStats, SwimNode,
};

pub use message_replay::{
    DeliveredMessage, MessageReplayReport, MessageReplayResult, ScheduledMessage,
    replay_message_schedule,
};

pub use metrics::{
    JoinMovementBreakdown, PlacementMetrics, TransitionMovementBreakdown,
    feasible_capacity_inclusion_targets, join_movement_breakdown, moved_bytes, movement_ratio,
    transition_movement_breakdown, zone_aware_capacity_inclusion_targets,
};
pub use migration::{
    MigrationBudget, MigrationError, MigrationPriority, MigrationScheduler, MigrationState,
    MigrationTask, NodeHealth, TabletAvailability, TickReport,
};
pub use model::{AdminState, Cluster, Node, NodeId, Placement, Tablet, TabletId};
pub use placement::{FailureDomainPolicy, PlacementStrategy};
pub use planner::{PlannerResult, plan_rebalance};

pub use runtime_coordinator::{
    CoordinatedResizeCommitOutcome, CoordinatedResizeDecision, TabletRuntimeCoordinator,
    TabletRuntimeError,
};

pub use reverse_index::{
    AdaptiveTabletIndex, AdaptiveTabletIndexKind, ContiguousTabletIndex, OpenAddressTabletIndex,
    ReverseIndexError, SortedTabletIndex, StdHashTabletIndex, TabletReverseIndex, TabletSlot,
};

pub use resize::{
    ResizeBlockReason, ResizeCommitOutcome, ResizeConfigError, ResizeDecision, ResizeKind,
    ResizePlan, TabletResizePolicy, TabletResizeState,
};

pub use range_resize::{
    HASH_SPACE_END, LifecycleResizeDecision, LifecycleResizeError, LifecycleResizePlan,
    RangeCommitOutcome, RangeResizeError, RangeResizeKind, RangeResizePlan, RangeTablet,
    RangeTabletMap, TabletRangeLifecycle,
};

pub use split_boundary::{
    RangeLoadSample, SplitBoundaryDecision, SplitBoundaryError, SplitBoundaryPolicy,
    SplitBoundaryStrategy, SplitPolicyDecision, SplitPolicyError, SplitPolicyReason,
    choose_split_boundary, choose_split_boundary_with_policy,
};

pub use transport::{
    DirectMigrationTransport, MessageAdvanceReport, MessageBusLimits, MessageClass, MessageSend,
    MigrationTransport, SimClock, SimMessage, SimNetwork, TransferPoll, TransferRequest,
    TransferSubmit,
};

pub use topology_snapshot::{
    DurableTopologyTxnWriter, PreparedGcState, PreparedTopologyGc, PreparedTopologyTxn,
    TopologyRecovery, TopologySnapshot, TopologySnapshotError, TopologyTxnError,
    TopologyTxnStartState, TopologyTxnState, read_current_topology, read_prepared_topology,
    recover_topology, validate_topology_txn_start,
};
