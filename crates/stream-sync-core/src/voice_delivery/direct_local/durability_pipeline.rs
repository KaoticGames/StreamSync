//! Cumulative ACK eligibility follows full durability pipeline (injection at each boundary).

use super::checkpoint::{
    atomic_replace_checkpoint_from_temp, write_checkpoint_temp, DirectCheckpointError,
    DirectCumulativeCheckpoint, NO_SEGMENTS_YET,
};
use super::generation_store::DirectGenerationStore;
use super::limits::{DirectReceiverLimits, DirectReceiverLimitsError, DirectReceiverPreflight};
use super::segment_identity::LogicalSegmentIdentity;
use super::synj_validate::validate_complete_synj_segment;
use crate::voice_delivery::fs::durability::{sync_dir_exact, sync_file, NamespaceDurability};
use crate::voice_delivery::fs::file::open_existing_file_at;
use crate::voice_delivery::fs::{FsError, VoiceFile};
use crate::voice_delivery::hash::hex_digest;
use sha2::{Digest, Sha256};
use std::fmt;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectDurabilityBoundary {
    BeforeSegmentFileSync,
    AfterSegmentPayloadWrite,
    BeforeLengthAndHashVerify,
    BeforeCheckpointTempWrite,
    BeforeCheckpointTempSync,
    BeforeCheckpointAtomicReplace,
    BeforeDirectoryNamespaceReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectSegmentDurabilityFailureKind {
    CheckpointAuthorityMismatch,
    IdentityBindingMismatch,
    NonContiguousSequence,
    SuppliedPayloadMismatch,
    ExistingSegmentConflict,
    SegmentWrite,
    SegmentSync,
    DurableSegmentVerification,
    CheckpointTempWrite,
    CheckpointTempSync,
    CheckpointReplace,
    NamespaceSync,
    NamespaceInconsistency,
    ReceiverLimitsExceeded,
    #[cfg(test)]
    InjectedTestFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectDurabilityReport {
    pub segment_write_completed: bool,
    pub segment_file_sync_completed: bool,
    pub length_and_hash_verified: bool,
    pub checkpoint_temp_write_completed: bool,
    pub checkpoint_temp_sync_completed: bool,
    pub checkpoint_atomic_replace_completed: bool,
    pub namespace_durability: NamespaceDurability,
    pub cumulative_ack_eligible: bool,
}

impl DirectDurabilityReport {
    pub fn claims_proven_namespace_durability(&self) -> bool {
        self.namespace_durability == NamespaceDurability::Proven
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectSegmentPersistOutcome {
    pub cumulative_ack_eligible: bool,
    pub highest_contiguous_sequence: u64,
}

#[derive(Debug)]
pub struct DirectSegmentDurabilityFailure {
    pub kind: DirectSegmentDurabilityFailureKind,
    pub cumulative_ack_eligible: bool,
    pub partial_report: DirectDurabilityReport,
}

impl fmt::Display for DirectSegmentDurabilityFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "direct segment durability incomplete")
    }
}

impl std::error::Error for DirectSegmentDurabilityFailure {}

#[cfg(test)]
type AfterSegmentFileSyncHook = Box<dyn Fn(&DirectGenerationStore, &str)>;

#[cfg(test)]
pub(crate) struct DirectDurabilityPipelineTestHooks {
    pub after_segment_file_sync: Option<AfterSegmentFileSyncHook>,
}

pub struct DirectSegmentDurabilityPipeline<'a> {
    store: &'a DirectGenerationStore,
    limits: DirectReceiverLimits,
    #[cfg(test)]
    injected_failure: Option<DirectDurabilityBoundary>,
    last_report: Option<DirectDurabilityReport>,
    #[cfg(test)]
    test_hooks: Option<DirectDurabilityPipelineTestHooks>,
}

impl<'a> DirectSegmentDurabilityPipeline<'a> {
    pub fn try_new(
        store: &'a DirectGenerationStore,
        limits: DirectReceiverLimits,
    ) -> Result<Self, DirectReceiverLimitsError> {
        DirectReceiverLimits::validate(&limits)?;
        Ok(Self {
            store,
            limits,
            #[cfg(test)]
            injected_failure: None,
            last_report: None,
            #[cfg(test)]
            test_hooks: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_test_hooks(mut self, hooks: DirectDurabilityPipelineTestHooks) -> Self {
        self.test_hooks = Some(hooks);
        self
    }

    #[cfg(test)]
    pub fn with_injected_failure_at(mut self, boundary: DirectDurabilityBoundary) -> Self {
        self.injected_failure = Some(boundary);
        self
    }

    pub fn last_durability_report(&self) -> Option<DirectDurabilityReport> {
        self.last_report.clone()
    }

    fn abort(
        &mut self,
        report: DirectDurabilityReport,
        kind: DirectSegmentDurabilityFailureKind,
    ) -> Result<DirectSegmentPersistOutcome, DirectSegmentDurabilityFailure> {
        self.last_report = Some(report.clone());
        Err(DirectSegmentDurabilityFailure {
            kind,
            cumulative_ack_eligible: false,
            partial_report: report,
        })
    }

    #[cfg(test)]
    fn abort_if_injected(
        &mut self,
        report: DirectDurabilityReport,
        boundary: DirectDurabilityBoundary,
    ) -> Result<(), DirectSegmentDurabilityFailure> {
        if self.injected_failure == Some(boundary) {
            self.abort(
                report,
                DirectSegmentDurabilityFailureKind::InjectedTestFailure,
            )?;
        }
        Ok(())
    }

    pub fn persist_for_cumulative_ack(
        &mut self,
        identity: &LogicalSegmentIdentity,
        bytes: &[u8],
        prior_checkpoint: &DirectCumulativeCheckpoint,
    ) -> Result<DirectSegmentPersistOutcome, DirectSegmentDurabilityFailure> {
        let mut report = fresh_report();

        if DirectReceiverPreflight::for_session_open(
            &self.limits,
            &identity.session_id,
            identity.delivery_generation,
            identity.byte_length,
            &identity.sha256_hex,
        )
        .is_err()
        {
            return self.abort(
                report,
                DirectSegmentDurabilityFailureKind::ReceiverLimitsExceeded,
            );
        }

        if prior_checkpoint.session_id != identity.session_id
            || prior_checkpoint.delivery_generation != identity.delivery_generation
        {
            return self.abort(
                report,
                DirectSegmentDurabilityFailureKind::IdentityBindingMismatch,
            );
        }
        if identity.byte_length != bytes.len() as u64 {
            return self.abort(
                report,
                DirectSegmentDurabilityFailureKind::SuppliedPayloadMismatch,
            );
        }
        let digest = hex_digest(&Sha256::digest(bytes));
        if digest != identity.sha256_hex {
            return self.abort(
                report,
                DirectSegmentDurabilityFailureKind::SuppliedPayloadMismatch,
            );
        }

        let prior_checkpoint = match self.reconcile_or_recover_checkpoint_authority(
            prior_checkpoint,
            identity,
            &mut report,
        ) {
            Ok(CheckpointAuthority::ContinueWithPrior(cp)) => cp,
            Ok(CheckpointAuthority::Recovered(outcome)) => {
                self.last_report = Some(report);
                return Ok(outcome);
            }
            Err(kind) => return self.abort(report, kind),
        };

        if prior_checkpoint.session_id != identity.session_id
            || prior_checkpoint.delivery_generation != identity.delivery_generation
        {
            return self.abort(
                report,
                DirectSegmentDurabilityFailureKind::IdentityBindingMismatch,
            );
        }
        if identity.segment_sequence != prior_checkpoint.next_expected_segment_sequence() {
            return self.abort(
                report,
                DirectSegmentDurabilityFailureKind::NonContiguousSequence,
            );
        }

        let segment_name = match self.store.segment_basename(identity) {
            Ok(n) => n,
            Err(_) => {
                return self.abort(
                    report,
                    DirectSegmentDurabilityFailureKind::IdentityBindingMismatch,
                );
            }
        };
        let generation_dir = self.store.generation_dir();

        enum SegmentDurability {
            WroteNew(VoiceFile),
            ResumedExisting(VoiceFile),
        }

        let segment_durability = match generation_dir.create_new_file(&segment_name) {
            Ok(mut segment_file) => {
                if segment_file.write_all_at(0, bytes).is_err() {
                    return self.abort(report, DirectSegmentDurabilityFailureKind::SegmentWrite);
                }
                SegmentDurability::WroteNew(segment_file)
            }
            Err(FsError::AlreadyExists) => {
                let segment_file = match open_existing_file_at(generation_dir, &segment_name) {
                    Ok(f) => f,
                    Err(_) => {
                        return self.abort(report, DirectSegmentDurabilityFailureKind::SegmentWrite)
                    }
                };
                SegmentDurability::ResumedExisting(segment_file)
            }
            Err(_) => return self.abort(report, DirectSegmentDurabilityFailureKind::SegmentWrite),
        };

        match segment_durability {
            SegmentDurability::WroteNew(segment_file) => {
                report.segment_write_completed = true;

                #[cfg(test)]
                self.abort_if_injected(
                    report.clone(),
                    DirectDurabilityBoundary::AfterSegmentPayloadWrite,
                )?;

                #[cfg(test)]
                self.abort_if_injected(
                    report.clone(),
                    DirectDurabilityBoundary::BeforeSegmentFileSync,
                )?;

                if sync_file(&segment_file).is_err() {
                    return self.abort(report, DirectSegmentDurabilityFailureKind::SegmentSync);
                }
                report.segment_file_sync_completed = true;

                #[cfg(test)]
                if let Some(hooks) = &self.test_hooks {
                    if let Some(after_sync) = &hooks.after_segment_file_sync {
                        after_sync(self.store, &segment_name);
                    }
                }

                #[cfg(test)]
                self.abort_if_injected(
                    report.clone(),
                    DirectDurabilityBoundary::BeforeLengthAndHashVerify,
                )?;

                if verify_durable_segment_bytes(generation_dir, &segment_name, identity).is_err() {
                    return self.abort(
                        report,
                        DirectSegmentDurabilityFailureKind::DurableSegmentVerification,
                    );
                }
                report.length_and_hash_verified = true;
            }
            SegmentDurability::ResumedExisting(segment_file) => {
                #[cfg(test)]
                self.abort_if_injected(
                    report.clone(),
                    DirectDurabilityBoundary::AfterSegmentPayloadWrite,
                )?;

                #[cfg(test)]
                self.abort_if_injected(
                    report.clone(),
                    DirectDurabilityBoundary::BeforeSegmentFileSync,
                )?;

                if sync_file(&segment_file).is_err() {
                    return self.abort(report, DirectSegmentDurabilityFailureKind::SegmentSync);
                }
                report.segment_file_sync_completed = true;

                #[cfg(test)]
                if let Some(hooks) = &self.test_hooks {
                    if let Some(after_sync) = &hooks.after_segment_file_sync {
                        after_sync(self.store, &segment_name);
                    }
                }

                #[cfg(test)]
                self.abort_if_injected(
                    report.clone(),
                    DirectDurabilityBoundary::BeforeLengthAndHashVerify,
                )?;

                if verify_durable_segment_bytes(generation_dir, &segment_name, identity).is_err() {
                    return self.abort(
                        report,
                        DirectSegmentDurabilityFailureKind::ExistingSegmentConflict,
                    );
                }
                report.length_and_hash_verified = true;
            }
        }

        #[cfg(test)]
        self.abort_if_injected(
            report.clone(),
            DirectDurabilityBoundary::BeforeCheckpointTempWrite,
        )?;

        let namespace_before_checkpoint = match sync_dir_exact(generation_dir) {
            Ok(ns) => ns,
            Err(_) => return self.abort(report, DirectSegmentDurabilityFailureKind::NamespaceSync),
        };

        let next_checkpoint = prior_checkpoint.advance_after_segment(
            identity.segment_sequence,
            identity.byte_length,
            &identity.sha256_hex,
            namespace_before_checkpoint,
        );

        let (temp_name, temp_file) = match write_checkpoint_temp(generation_dir, &next_checkpoint) {
            Ok(pair) => pair,
            Err(_) => {
                return self.abort(
                    report,
                    DirectSegmentDurabilityFailureKind::CheckpointTempWrite,
                );
            }
        };
        report.checkpoint_temp_write_completed = true;

        #[cfg(test)]
        if self.injected_failure == Some(DirectDurabilityBoundary::BeforeCheckpointTempSync) {
            let _ = generation_dir.remove_child_file(&temp_name);
            return self.abort(
                report,
                DirectSegmentDurabilityFailureKind::InjectedTestFailure,
            );
        }
        if sync_file(&temp_file).is_err() {
            let _ = generation_dir.remove_child_file(&temp_name);
            return self.abort(
                report,
                DirectSegmentDurabilityFailureKind::CheckpointTempSync,
            );
        }
        report.checkpoint_temp_sync_completed = true;

        #[cfg(test)]
        if self.injected_failure == Some(DirectDurabilityBoundary::BeforeCheckpointAtomicReplace) {
            let _ = generation_dir.remove_child_file(&temp_name);
            return self.abort(
                report,
                DirectSegmentDurabilityFailureKind::InjectedTestFailure,
            );
        }

        if atomic_replace_checkpoint_from_temp(generation_dir, &temp_name).is_err() {
            let _ = generation_dir.remove_child_file(&temp_name);
            return self.abort(
                report,
                DirectSegmentDurabilityFailureKind::CheckpointReplace,
            );
        }
        report.checkpoint_atomic_replace_completed = true;

        #[cfg(test)]
        self.abort_if_injected(
            report.clone(),
            DirectDurabilityBoundary::BeforeDirectoryNamespaceReport,
        )?;

        let namespace_after_replace = match sync_dir_exact(generation_dir) {
            Ok(ns) => ns,
            Err(_) => return self.abort(report, DirectSegmentDurabilityFailureKind::NamespaceSync),
        };
        if namespace_after_replace != next_checkpoint.namespace_durability {
            return self.abort(
                report,
                DirectSegmentDurabilityFailureKind::NamespaceInconsistency,
            );
        }
        report.namespace_durability = namespace_after_replace;

        report.cumulative_ack_eligible = true;
        let outcome = DirectSegmentPersistOutcome {
            cumulative_ack_eligible: true,
            highest_contiguous_sequence: identity.segment_sequence,
        };
        self.last_report = Some(report);
        Ok(outcome)
    }

    fn reconcile_or_recover_checkpoint_authority(
        &mut self,
        supplied_prior: &DirectCumulativeCheckpoint,
        identity: &LogicalSegmentIdentity,
        report: &mut DirectDurabilityReport,
    ) -> Result<CheckpointAuthority, DirectSegmentDurabilityFailureKind> {
        if let Ok(durable) = DirectCumulativeCheckpoint::read_durable_from_store(self.store) {
            if durable != *supplied_prior {
                if durable_is_one_step_ahead_of_supplied(supplied_prior, &durable, identity) {
                    let outcome = self
                        .finish_ack_after_durable_checkpoint_replace(&durable, identity, report)?;
                    return Ok(CheckpointAuthority::Recovered(outcome));
                }
                return Err(DirectSegmentDurabilityFailureKind::CheckpointAuthorityMismatch);
            }
        }

        match DirectCumulativeCheckpoint::reconcile_supplied_with_durable_authority(
            self.store,
            supplied_prior,
        ) {
            Ok(cp) => Ok(CheckpointAuthority::ContinueWithPrior(cp)),
            Err(DirectCheckpointError::IdentityMismatch) => {
                Err(DirectSegmentDurabilityFailureKind::CheckpointAuthorityMismatch)
            }
            Err(DirectCheckpointError::ChecksumMismatch | DirectCheckpointError::NotFound) => {
                Err(DirectSegmentDurabilityFailureKind::CheckpointAuthorityMismatch)
            }
            Err(DirectCheckpointError::Fs(_)) => {
                Err(DirectSegmentDurabilityFailureKind::CheckpointAuthorityMismatch)
            }
        }
    }

    fn finish_ack_after_durable_checkpoint_replace(
        &mut self,
        durable_checkpoint: &DirectCumulativeCheckpoint,
        identity: &LogicalSegmentIdentity,
        report: &mut DirectDurabilityReport,
    ) -> Result<DirectSegmentPersistOutcome, DirectSegmentDurabilityFailureKind> {
        let segment_name = self
            .store
            .segment_basename(identity)
            .map_err(|_| DirectSegmentDurabilityFailureKind::IdentityBindingMismatch)?;
        let generation_dir = self.store.generation_dir();

        let segment_file = open_existing_file_at(generation_dir, &segment_name)
            .map_err(|_| DirectSegmentDurabilityFailureKind::DurableSegmentVerification)?;

        if sync_file(&segment_file).is_err() {
            return Err(DirectSegmentDurabilityFailureKind::SegmentSync);
        }
        report.segment_file_sync_completed = true;

        if verify_durable_segment_bytes(generation_dir, &segment_name, identity).is_err() {
            return Err(DirectSegmentDurabilityFailureKind::DurableSegmentVerification);
        }
        report.length_and_hash_verified = true;
        report.checkpoint_atomic_replace_completed = true;

        let namespace_after_replace = sync_dir_exact(generation_dir)
            .map_err(|_| DirectSegmentDurabilityFailureKind::NamespaceSync)?;
        if namespace_after_replace != durable_checkpoint.namespace_durability {
            return Err(DirectSegmentDurabilityFailureKind::NamespaceInconsistency);
        }
        report.namespace_durability = namespace_after_replace;
        report.cumulative_ack_eligible = true;

        Ok(DirectSegmentPersistOutcome {
            cumulative_ack_eligible: true,
            highest_contiguous_sequence: identity.segment_sequence,
        })
    }
}

enum CheckpointAuthority {
    ContinueWithPrior(DirectCumulativeCheckpoint),
    Recovered(DirectSegmentPersistOutcome),
}

fn durable_is_one_step_ahead_of_supplied(
    supplied: &DirectCumulativeCheckpoint,
    durable: &DirectCumulativeCheckpoint,
    identity: &LogicalSegmentIdentity,
) -> bool {
    if supplied.session_id != durable.session_id
        || supplied.delivery_generation != durable.delivery_generation
    {
        return false;
    }
    if supplied.next_expected_segment_sequence() != identity.segment_sequence {
        return false;
    }
    if durable.highest_contiguous_sequence != identity.segment_sequence {
        return false;
    }
    if durable.last_segment_byte_length != identity.byte_length
        || durable.last_segment_sha256_hex != identity.sha256_hex
    {
        return false;
    }
    if supplied.highest_contiguous_sequence == NO_SEGMENTS_YET {
        identity.segment_sequence == 0
    } else {
        durable.highest_contiguous_sequence == supplied.highest_contiguous_sequence + 1
    }
}

fn verify_durable_segment_bytes(
    generation_dir: &crate::voice_delivery::fs::DirHandle,
    segment_name: &str,
    identity: &LogicalSegmentIdentity,
) -> Result<(), FsError> {
    let durable = generation_dir.read_file_all(segment_name)?;
    validate_complete_synj_segment(&durable).map_err(|_| {
        FsError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid synj on durable segment",
        ))
    })?;
    if durable.len() as u64 != identity.byte_length {
        return Err(FsError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "durable segment length mismatch",
        )));
    }
    let durable_digest = hex_digest(&Sha256::digest(&durable));
    if durable_digest != identity.sha256_hex {
        return Err(FsError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "durable segment digest mismatch",
        )));
    }
    Ok(())
}

fn fresh_report() -> DirectDurabilityReport {
    DirectDurabilityReport {
        segment_write_completed: false,
        segment_file_sync_completed: false,
        length_and_hash_verified: false,
        checkpoint_temp_write_completed: false,
        checkpoint_temp_sync_completed: false,
        checkpoint_atomic_replace_completed: false,
        namespace_durability: NamespaceDurability::Unavailable,
        cumulative_ack_eligible: false,
    }
}

pub struct DirectNamespaceDurabilityProbe<'a> {
    store: &'a DirectGenerationStore,
}

impl<'a> DirectNamespaceDurabilityProbe<'a> {
    pub fn for_store(store: &'a DirectGenerationStore) -> Self {
        Self { store }
    }

    pub fn report_after_segment_and_checkpoint_durability(
        &self,
    ) -> Result<DirectDurabilityReport, DirectCheckpointError> {
        let ns =
            sync_dir_exact(self.store.generation_dir()).unwrap_or(NamespaceDurability::Unavailable);
        Ok(DirectDurabilityReport {
            segment_write_completed: false,
            segment_file_sync_completed: false,
            length_and_hash_verified: false,
            checkpoint_temp_write_completed: false,
            checkpoint_temp_sync_completed: false,
            checkpoint_atomic_replace_completed: false,
            namespace_durability: ns,
            cumulative_ack_eligible: false,
        })
    }
}
