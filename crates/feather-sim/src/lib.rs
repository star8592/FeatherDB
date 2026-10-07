#![forbid(unsafe_code)]

pub mod compact;
pub mod compact_catalog;
pub mod compact_migration;
pub mod compact_window;
pub mod fault;
mod hash;
pub mod metrics;
pub mod migration;
pub mod model;
pub mod placement;
pub mod planner;
pub mod range_resize;
pub mod resize;
pub mod split_boundary;
pub mod transport;

pub use compact::{CompactPlacement, CompactPlacementError};
pub use compact_catalog::{CompactCatalogError, CompactTabletCatalog};
pub use compact_migration::{CompactMigrationCursor, CompactMigrationMove};
pub use compact_window::{
    CompactWindowError, CompactWindowPhase, CompactWindowReconcileReport, CompactWindowScheduler,
};

pub use fault::{
    FaultAction, FaultEvent, FaultReplayReport, FaultTrace, FaultTraceParseError,
    replay_migration_faults, replay_migration_faults_with_network,
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
    DirectMigrationTransport, MigrationTransport, SimClock, SimNetwork, TransferPoll,
    TransferRequest, TransferSubmit,
};
