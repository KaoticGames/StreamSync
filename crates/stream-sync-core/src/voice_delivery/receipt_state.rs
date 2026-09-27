//! Durable local receipt lifecycle (immutable generations under control/receipt).

pub use crate::voice_delivery::records::receipt_generation::{
    ReceiptGenerationError, ReceiptHistoryState, ReceiptPhase, ReceiptStore,
};
