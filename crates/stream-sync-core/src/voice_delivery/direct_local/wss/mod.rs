//! Outbound direct-local WSS client (slice 2, vertical 2). Library-only; not wired to AppState.

pub mod auth;
pub mod protocol;
pub mod segment_fsm;
pub mod session;
pub mod transport;
