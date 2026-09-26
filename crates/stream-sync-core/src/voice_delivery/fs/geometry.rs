//! Stage/final sibling geometry under one held final-parent handle.

use super::dir::{DirHandle, ValidatedFinalName};
use super::error::FsError;
use super::rename_no_replace_same_parent;
use crate::voice_delivery::ids::{new_opaque_stage_basename, validate_stage_basename};

/// Publication operations constrained to a single open final-parent directory.
pub struct FinalParentPublication {
    final_parent: DirHandle,
}

impl FinalParentPublication {
    pub fn new(final_parent: DirHandle) -> Self {
        Self { final_parent }
    }

    pub(crate) fn final_parent(&self) -> &DirHandle {
        &self.final_parent
    }

    /// Create `.streamsync-stage-<random128>` as a direct child of the final-parent handle.
    pub fn create_stage_dir(&self) -> Result<String, FsError> {
        let name = new_opaque_stage_basename();
        self.final_parent.create_child_dir(&name)?;
        Ok(name)
    }

    /// Rename an existing stage directory to the validated final session name (same parent fd).
    pub fn rename_stage_to_final(
        &self,
        stage_basename: &str,
        final_name: &ValidatedFinalName,
    ) -> Result<(), FsError> {
        validate_stage_basename(stage_basename)?;
        rename_no_replace_same_parent(&self.final_parent, stage_basename, final_name.as_str())
    }
}
