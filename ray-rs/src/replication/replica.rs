//! Replica-side bootstrap/pull/apply orchestration support.

use super::log_store::{ReplicationFrame, SegmentLogStore};
use super::manifest::{ManifestStore, ReplicationManifest};
use super::primary::default_replication_sidecar_path;
use super::types::ReplicationRole;
use crate::error::{KiteError, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const MANIFEST_FILE_NAME: &str = "manifest.json";
const CURSOR_FILE_NAME: &str = "replica-cursor.json";

#[derive(Debug, Clone)]
pub struct ReplicaReplicationStatus {
  pub role: ReplicationRole,
  pub source_db_path: Option<PathBuf>,
  pub source_sidecar_path: Option<PathBuf>,
  pub applied_epoch: u64,
  pub applied_log_index: u64,
  pub last_error: Option<String>,
  pub needs_reseed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ReplicaCursorState {
  applied_epoch: u64,
  applied_log_index: u64,
  last_error: Option<String>,
  needs_reseed: bool,
}

#[derive(Debug)]
pub struct ReplicaReplication {
  local_sidecar_path: PathBuf,
  cursor_state_path: PathBuf,
  source_db_path: Option<PathBuf>,
  source_sidecar_path: Option<PathBuf>,
  state: Mutex<ReplicaCursorState>,
}

impl ReplicaReplication {
  pub fn open(
    replica_db_path: &Path,
    local_sidecar_path: Option<PathBuf>,
    source_db_path: Option<PathBuf>,
    source_sidecar_path: Option<PathBuf>,
  ) -> Result<Self> {
    let local_sidecar_path =
      local_sidecar_path.unwrap_or_else(|| default_replication_sidecar_path(replica_db_path));
    std::fs::create_dir_all(&local_sidecar_path)?;

    let cursor_state_path = local_sidecar_path.join(CURSOR_FILE_NAME);
    let state = load_cursor_state(&cursor_state_path)?;

    let source_db_path = source_db_path.ok_or_else(|| {
      KiteError::InvalidReplication("replica source db path is not configured".to_string())
    })?;
    if !source_db_path.exists() {
      return Err(KiteError::InvalidReplication(format!(
        "replica source db path does not exist: {}",
        source_db_path.display()
      )));
    }
    if source_db_path.is_dir() {
      return Err(KiteError::InvalidReplication(format!(
        "replica source db path must be a file: {}",
        source_db_path.display()
      )));
    }
    if paths_equivalent(replica_db_path, &source_db_path) {
      return Err(KiteError::InvalidReplication(
        "replica source db path must differ from replica db path".to_string(),
      ));
    }

    let source_sidecar_path =
      source_sidecar_path.or_else(|| Some(default_replication_sidecar_path(&source_db_path)));
    if let Some(path) = source_sidecar_path.as_ref() {
      if path.exists() && !path.is_dir() {
        return Err(KiteError::InvalidReplication(format!(
          "replica source sidecar path must be a directory: {}",
          path.display()
        )));
      }
      if paths_equivalent(path, &local_sidecar_path) {
        return Err(KiteError::InvalidReplication(
          "replica source sidecar path must differ from local sidecar path".to_string(),
        ));
      }
    }

    Ok(Self {
      local_sidecar_path,
      cursor_state_path,
      source_db_path: Some(source_db_path),
      source_sidecar_path,
      state: Mutex::new(state),
    })
  }

  pub fn source_db_path(&self) -> Option<PathBuf> {
    self.source_db_path.clone()
  }

  pub fn source_sidecar_path(&self) -> Option<PathBuf> {
    self.source_sidecar_path.clone()
  }

  pub fn applied_position(&self) -> (u64, u64) {
    let state = self.state.lock();
    (state.applied_epoch, state.applied_log_index)
  }

  pub fn source_head_position(&self) -> Result<(u64, u64)> {
    let source_sidecar_path = self.source_sidecar_path.as_ref().ok_or_else(|| {
      KiteError::InvalidReplication("replica source sidecar path is not configured".to_string())
    })?;

    let manifest = ManifestStore::new(source_sidecar_path.join(MANIFEST_FILE_NAME)).read()?;
    Ok((manifest.epoch, manifest.head_log_index))
  }

  pub fn mark_applied(&self, epoch: u64, log_index: u64) -> Result<()> {
    let mut state = self.state.lock();

    if state.applied_epoch > epoch
      || (state.applied_epoch == epoch && state.applied_log_index > log_index)
    {
      return Err(KiteError::InvalidReplication(format!(
        "attempted to move replica cursor backwards: {}:{} -> {}:{}",
        state.applied_epoch, state.applied_log_index, epoch, log_index
      )));
    }

    state.applied_epoch = epoch;
    state.applied_log_index = log_index;
    state.last_error = None;
    state.needs_reseed = false;
    persist_cursor_state(&self.cursor_state_path, &state)
  }

  pub fn mark_error(&self, message: impl Into<String>, needs_reseed: bool) -> Result<()> {
    let mut state = self.state.lock();
    state.last_error = Some(message.into());
    state.needs_reseed = needs_reseed;
    persist_cursor_state(&self.cursor_state_path, &state)
  }

  pub fn clear_error(&self) -> Result<()> {
    let mut state = self.state.lock();
    if state.last_error.is_none() && !state.needs_reseed {
      return Ok(());
    }
    state.last_error = None;
    state.needs_reseed = false;
    persist_cursor_state(&self.cursor_state_path, &state)
  }

  pub fn status(&self) -> ReplicaReplicationStatus {
    let state = self.state.lock();
    ReplicaReplicationStatus {
      role: ReplicationRole::Replica,
      source_db_path: self.source_db_path.clone(),
      source_sidecar_path: self.source_sidecar_path.clone(),
      applied_epoch: state.applied_epoch,
      applied_log_index: state.applied_log_index,
      last_error: state.last_error.clone(),
      needs_reseed: state.needs_reseed,
    }
  }

  pub fn frames_after(
    &self,
    max_frames: usize,
    include_last_applied: bool,
  ) -> Result<Vec<ReplicationFrame>> {
    let source_sidecar_path = self.source_sidecar_path.as_ref().ok_or_else(|| {
      KiteError::InvalidReplication("replica source sidecar path is not configured".to_string())
    })?;

    let manifest = ManifestStore::new(source_sidecar_path.join(MANIFEST_FILE_NAME)).read()?;
    let all_frames = read_all_frames(source_sidecar_path, &manifest)?;

    let (applied_epoch, applied_log_index) = self.applied_position();
    if manifest.epoch == applied_epoch && applied_log_index < manifest.retained_floor {
      let message = format!(
        "replica needs reseed: applied log {} is below retained floor {}",
        applied_log_index, manifest.retained_floor
      );
      self.mark_error(message.clone(), true)?;
      return Err(KiteError::InvalidReplication(message));
    }

    let mut filtered: Vec<ReplicationFrame> = all_frames
      .into_iter()
      .filter(|frame| {
        if frame.epoch > applied_epoch {
          return true;
        }
        if frame.epoch < applied_epoch {
          return false;
        }

        if include_last_applied && applied_log_index > 0 {
          frame.log_index >= applied_log_index
        } else {
          frame.log_index > applied_log_index
        }
      })
      .collect();

    filtered.sort_by(|left, right| {
      left
        .epoch
        .cmp(&right.epoch)
        .then_with(|| left.log_index.cmp(&right.log_index))
    });

    let expected_next_log = applied_log_index.saturating_add(1);
    if let Some(first) = filtered.first() {
      if first.epoch == applied_epoch && first.log_index > expected_next_log {
        let message = format!(
          "replica needs reseed: missing log range {}..{}",
          expected_next_log,
          first.log_index.saturating_sub(1)
        );
        self.mark_error(message.clone(), true)?;
        return Err(KiteError::InvalidReplication(message));
      }
    }

    if filtered.is_empty() && manifest.head_log_index > applied_log_index {
      let message = format!(
        "replica needs reseed: applied log {} but primary head is {} and required frames are unavailable",
        applied_log_index, manifest.head_log_index
      );
      self.mark_error(message.clone(), true)?;
      return Err(KiteError::InvalidReplication(message));
    }

    if max_frames > 0 && filtered.len() > max_frames {
      filtered.truncate(max_frames);
    }

    Ok(filtered)
  }

  pub fn local_sidecar_path(&self) -> &Path {
    &self.local_sidecar_path
  }
}

fn load_cursor_state(path: &Path) -> Result<ReplicaCursorState> {
  if !path.exists() {
    return Ok(ReplicaCursorState::default());
  }

  let bytes = std::fs::read(path)?;
  let state: ReplicaCursorState = serde_json::from_slice(&bytes).map_err(|error| {
    KiteError::Serialization(format!("decode replica cursor state failed: {error}"))
  })?;
  Ok(state)
}

fn persist_cursor_state(path: &Path, state: &ReplicaCursorState) -> Result<()> {
  let tmp_path = path.with_extension("json.tmp");
  let bytes = serde_json::to_vec(state).map_err(|error| {
    KiteError::Serialization(format!("encode replica cursor state failed: {error}"))
  })?;

  std::fs::write(&tmp_path, &bytes)?;
  std::fs::rename(&tmp_path, path)?;
  Ok(())
}

fn read_all_frames(
  sidecar_path: &Path,
  manifest: &ReplicationManifest,
) -> Result<Vec<ReplicationFrame>> {
  let mut segments = manifest.segments.clone();
  segments.sort_by_key(|segment| segment.id);

  let mut frames = Vec::new();
  for segment in segments {
    let segment_path = sidecar_path.join(segment_file_name(segment.id));
    if !segment_path.exists() {
      continue;
    }

    let segment_frames = SegmentLogStore::open(&segment_path)?.read_all()?;
    frames.extend(segment_frames);
  }

  frames.sort_by(|left, right| {
    left
      .epoch
      .cmp(&right.epoch)
      .then_with(|| left.log_index.cmp(&right.log_index))
  });

  Ok(frames)
}

fn segment_file_name(id: u64) -> String {
  format!("segment-{id:020}.rlog")
}

fn normalize_path_for_compare(path: &Path) -> PathBuf {
  let absolute = if path.is_absolute() {
    path.to_path_buf()
  } else {
    match std::env::current_dir() {
      Ok(cwd) => cwd.join(path),
      Err(_) => path.to_path_buf(),
    }
  };
  std::fs::canonicalize(&absolute).unwrap_or(absolute)
}

fn paths_equivalent(left: &Path, right: &Path) -> bool {
  normalize_path_for_compare(left) == normalize_path_for_compare(right)
}
