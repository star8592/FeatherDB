#![forbid(unsafe_code)]

mod hash;
pub mod metrics;
pub mod migration;
pub mod model;
pub mod placement;
pub mod planner;
pub mod resize;

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
