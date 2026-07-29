pub mod detector;
pub mod resolver;

pub use detector::{
    conflicts_between, detect_conflicts, detect_nway_conflicts, Conflict, NWayConflict, QueuedSlot,
};
pub use resolver::{
    resolve_conflict, resolve_nway_conflict, CandidateEvidence, ConflictEvidence, RelayObservation,
};
