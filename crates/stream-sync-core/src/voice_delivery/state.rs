//! Pure ledger state transition rules.

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LedgerState {
    Receiving,
    Sealed,
    PublishIntent,
    Published,
    Quarantined,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransitionError {
    #[error("illegal backward transition")]
    Backward,
    #[error("illegal skip transition")]
    Skip,
    #[error("terminal quarantined state does not advance")]
    Terminal,
}

/// Monotonic: Absent→Receiving→Sealed→PublishIntent→Published; Quarantined is terminal.
pub fn validate_ledger_transition(
    from: Option<LedgerState>,
    to: LedgerState,
) -> Result<(), TransitionError> {
    if to == LedgerState::Quarantined {
        return Ok(());
    }
    match from {
        None => {
            if to == LedgerState::Receiving {
                Ok(())
            } else {
                Err(TransitionError::Skip)
            }
        }
        Some(LedgerState::Receiving) => match to {
            LedgerState::Receiving | LedgerState::Sealed | LedgerState::Quarantined => Ok(()),
            _ => Err(TransitionError::Skip),
        },
        Some(LedgerState::Sealed) => match to {
            LedgerState::Sealed | LedgerState::PublishIntent | LedgerState::Quarantined => Ok(()),
            _ => Err(TransitionError::Skip),
        },
        Some(LedgerState::PublishIntent) => match to {
            LedgerState::PublishIntent | LedgerState::Published | LedgerState::Quarantined => {
                Ok(())
            }
            _ => Err(TransitionError::Skip),
        },
        Some(LedgerState::Published) => match to {
            LedgerState::Published => Ok(()),
            LedgerState::Quarantined => Ok(()),
            _ => Err(TransitionError::Backward),
        },
        Some(LedgerState::Quarantined) => Err(TransitionError::Terminal),
    }
}

#[cfg(test)]
mod ledger_transition_table {
    use super::*;

    #[test]
    fn forward_path_allowed() {
        assert!(validate_ledger_transition(None, LedgerState::Receiving).is_ok());
        assert!(
            validate_ledger_transition(Some(LedgerState::Receiving), LedgerState::Sealed).is_ok()
        );
        assert!(
            validate_ledger_transition(Some(LedgerState::Sealed), LedgerState::PublishIntent)
                .is_ok()
        );
        assert!(validate_ledger_transition(
            Some(LedgerState::PublishIntent),
            LedgerState::Published
        )
        .is_ok());
    }

    #[test]
    fn backward_and_skip_rejected() {
        assert!(matches!(
            validate_ledger_transition(Some(LedgerState::Published), LedgerState::Receiving),
            Err(TransitionError::Backward)
        ));
        assert!(matches!(
            validate_ledger_transition(Some(LedgerState::Receiving), LedgerState::Published),
            Err(TransitionError::Skip)
        ));
        assert!(matches!(
            validate_ledger_transition(Some(LedgerState::Quarantined), LedgerState::Receiving),
            Err(TransitionError::Terminal)
        ));
    }
}
