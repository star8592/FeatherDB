#![forbid(unsafe_code)]

mod hash;
pub mod metrics;
pub mod model;
pub mod placement;
pub mod planner;

pub use metrics::{
    JoinMovementBreakdown, PlacementMetrics, TransitionMovementBreakdown,
    feasible_capacity_inclusion_targets, join_movement_breakdown, moved_bytes, movement_ratio,
    transition_movement_breakdown, zone_aware_capacity_inclusion_targets,
};
pub use model::{AdminState, Cluster, Node, NodeId, Placement, Tablet, TabletId};
pub use placement::{FailureDomainPolicy, PlacementStrategy};
pub use planner::{PlannerResult, plan_rebalance};
