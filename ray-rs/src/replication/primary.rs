//! Primary-side replication orchestration.

use super::log_store::SegmentLogStore;
use super::manifest::{ManifestStore, ReplicationManifest, SegmentMeta, MANIFEST_ENVELOPE_VERSION};
use super::progress::{
  clear_replica_progress, load_replica_progress, upsert_replica_progress,
  ReplicaProgress as ReplicaProgressEntry,
};
use super::transport::build_commit_payload_header;
use super::types::{CommitToken, ReplicationRole};
use crate::core::single_file::SyncMode;
use crate::error::{KiteError, Result};
use fs2::FileExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MANIFEST_FILE_NAME: &str = "manifest.json";
const PRIMARY_LOCK_FILE_NAME: &str = "primary.lock";
const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_RETENTION_MIN_ENTRIES: u64 = 1024;
const DEFAULT_MANIFEST_REFRESH_APPEND_INTERVAL: u64 = 256;
const DEFAULT_APPEND_WRITE_BUFFER_BYTES: usize = 16 * 1024 * 1024;

type SidecarOpLock = Arc<Mutex<()>>;
type SidecarPrimaryLock = Arc<PrimarySidecarProcessLock>;
type SidecarEpochFence = Arc<AtomicU64>;

static SIDECAR_LOCKS: OnceLock<StdMutex<HashMap<PathBuf, SidecarOpLock>>> = OnceLock::new();
static SIDECAR_PRIMARY_LOCKS: OnceLock<
  StdMutex<HashMap<PathBuf, Weak<PrimarySidecarProcessLock>>>,
> = OnceLock::new();
static SIDECAR_EPOCH_FENCES: OnceLock<StdMutex<HashMap<PathBuf, Weak<AtomicU64>>>> =
  OnceLock::new();

#[derive(Debug, Clone)]
pub struct PrimaryReplicationStatus {
  pub role: ReplicationRole,
  pub epoch: u64,
  pub head_log_index: u64,
  pub retained_floor: u64,
  pub replica_lags: Vec<ReplicaLagStatus>,
  pub sidecar_path: PathBuf,
  pub last_token: Option<CommitToken>,
  pub append_attempts: u64,
  pub append_failures: u64,
  pub append_successes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaLagStatus {
  pub replica_id: String,
  pub epoch: u64,
  pub applied_log_index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryRetentionOutcome {
  pub pruned_segments: usize,
  pub retained_floor: u64,
}

#[derive(Debug)]
struct PrimarySidecarProcessLock {
  _file: File,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ManifestDiskStamp {
  len: u64,
  modified_unix_nanos: Option<u128>,
}

#[derive(Debug)]
struct PrimaryReplicationState {
  manifest: ReplicationManifest,
  manifest_disk_stamp: ManifestDiskStamp,
  log_store: SegmentLogStore,
  active_segment_size_bytes: u64,
  last_token: Option<CommitToken>,
  replica_progress: HashMap<String, ReplicaProgressEntry>,
  write_fenced: bool,
  appends_since_manifest_refresh: u64,
}

#[derive(Debug)]
pub struct PrimaryReplication {
  sidecar_path: PathBuf,
  manifest_store: ManifestStore,
  state: Mutex<PrimaryReplicationState>,
  append_attempts: AtomicU64,
  append_failures: AtomicU64,
  append_successes: AtomicU64,
  segment_max_bytes: u64,
  retention_min_entries: u64,
  retention_min_duration: Option<Duration>,
  durable_append: bool,
  checksum_payload: bool,
  persist_manifest_each_append: bool,
  manifest_refresh_append_interval: u64,
  append_write_buffer_bytes: usize,
  fail_after_append_for_testing: Option<u64>,
  sidecar_op_lock: SidecarOpLock,
  _sidecar_primary_lock: SidecarPrimaryLock,
  epoch_fence: SidecarEpochFence,
}

impl PrimaryReplication {
  pub fn open(
    db_path: &Path,
    sidecar_path: Option<PathBuf>,
    segment_max_bytes: Option<u64>,
    retention_min_entries: Option<u64>,
    retention_min_ms: Option<u64>,
    sync_mode: SyncMode,
    fail_after_append_for_testing: Option<u64>,
  ) -> Result<Self> {
    let sidecar_path =
      sidecar_path.unwrap_or_else(|| default_replication_sidecar_path(db_path.as_ref()));
    std::fs::create_dir_all(&sidecar_path)?;
    let sidecar_primary_lock = acquire_sidecar_primary_lock(&sidecar_path)?;

    let manifest_store = ManifestStore::new(sidecar_path.join(MANIFEST_FILE_NAME));

    let mut manifest = if manifest_store.path().exists() {
      manifest_store.read()?
    } else {
      let initial = ReplicationManifest {
        version: MANIFEST_ENVELOPE_VERSION,
        epoch: 1,
        head_log_index: 0,
        retained_floor: 0,
        active_segment_id: 1,
        segments: vec![SegmentMeta {
          id: 1,
          start_log_index: 1,
          end_log_index: 0,
          size_bytes: 0,
        }],
      };
      manifest_store.write(&initial)?;
      initial
    };

    ensure_active_segment_metadata(&mut manifest);
    if reconcile_manifest_head_from_active_segment(&sidecar_path, &mut manifest)? {
      // Recover append state when manifest head lagged a flushed segment tail.
      manifest_store.write(&manifest)?;
    }

    let segment_path = sidecar_path.join(segment_file_name(manifest.active_segment_id));
    let active_segment_size_bytes = segment_file_len(&segment_path)?;
    let append_write_buffer_bytes = if matches!(sync_mode, SyncMode::Full) {
      0
    } else {
      DEFAULT_APPEND_WRITE_BUFFER_BYTES
    };
    let log_store =
      SegmentLogStore::open_or_create_append_with_buffer(&segment_path, append_write_buffer_bytes)?;
    let manifest_disk_stamp = read_manifest_disk_stamp(manifest_store.path())?;
    let replica_progress = load_replica_progress(&sidecar_path)?;

    let sidecar_op_lock = sidecar_operation_lock(&sidecar_path);
    let epoch_fence = sidecar_epoch_fence(&sidecar_path, manifest.epoch);

    Ok(Self {
      sidecar_path,
      manifest_store,
      state: Mutex::new(PrimaryReplicationState {
        manifest,
        manifest_disk_stamp,
        log_store,
        active_segment_size_bytes,
        last_token: None,
        replica_progress,
        write_fenced: false,
        appends_since_manifest_refresh: 0,
      }),
      append_attempts: AtomicU64::new(0),
      append_failures: AtomicU64::new(0),
      append_successes: AtomicU64::new(0),
      segment_max_bytes: segment_max_bytes
        .unwrap_or(DEFAULT_SEGMENT_MAX_BYTES)
        .max(1),
      retention_min_entries: retention_min_entries.unwrap_or(DEFAULT_RETENTION_MIN_ENTRIES),
      retention_min_duration: retention_min_ms.map(Duration::from_millis),
      durable_append: matches!(sync_mode, SyncMode::Full),
      checksum_payload: matches!(sync_mode, SyncMode::Full),
      persist_manifest_each_append: matches!(sync_mode, SyncMode::Full),
      manifest_refresh_append_interval: if matches!(sync_mode, SyncMode::Full) {
        1
      } else {
        DEFAULT_MANIFEST_REFRESH_APPEND_INTERVAL
      },
      append_write_buffer_bytes,
      fail_after_append_for_testing,
      sidecar_op_lock,
      _sidecar_primary_lock: sidecar_primary_lock,
      epoch_fence,
    })
  }

  pub fn append_commit_frame(&self, payload: Vec<u8>) -> Result<CommitToken> {
    self.append_commit_payload_segments(&[payload.as_slice()])
  }

  pub fn append_commit_wal_frame(&self, txid: u64, wal_bytes: Vec<u8>) -> Result<CommitToken> {
    let header = build_commit_payload_header(txid, wal_bytes.len())?;
    self.append_commit_payload_owned_segments(vec![header.to_vec(), wal_bytes])
  }

  fn append_commit_payload_segments(&self, payload_segments: &[&[u8]]) -> Result<CommitToken> {
    self.append_attempts.fetch_add(1, Ordering::Relaxed);

    if let Some(limit) = self.fail_after_append_for_testing {
      let successes = self.append_successes.load(Ordering::Relaxed);
      if successes >= limit {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(KiteError::InvalidReplication(
          "replication append failure injected for testing".to_string(),
        ));
      }
    }

    let _sidecar_guard = self.sidecar_op_lock.lock();
    let mut state = self.state.lock();
    let fenced_epoch = self.epoch_fence.load(Ordering::Acquire);
    if fenced_epoch > state.manifest.epoch {
      state.write_fenced = true;
      self.append_failures.fetch_add(1, Ordering::Relaxed);
      return Err(stale_primary_error());
    }
    if state.write_fenced {
      self.append_failures.fetch_add(1, Ordering::Relaxed);
      return Err(stale_primary_error());
    }
    let should_refresh = state.appends_since_manifest_refresh
      >= self.manifest_refresh_append_interval.saturating_sub(1);
    if should_refresh {
      let epoch_changed = self.refresh_manifest_locked(&mut state)?;
      state.appends_since_manifest_refresh = 0;
      if epoch_changed || state.write_fenced {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(stale_primary_error());
      }
    }

    let epoch = state.manifest.epoch;
    let next_log_index = state.manifest.head_log_index.saturating_add(1);

    let frame_size = match state.log_store.append_payload_segments_with_crc(
      epoch,
      next_log_index,
      payload_segments,
      self.checksum_payload,
    ) {
      Ok(size) => size,
      Err(error) => {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(error);
      }
    };

    if self.durable_append {
      if let Err(error) = state.log_store.sync() {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(error);
      }
    }

    let mut next_manifest = state.manifest.clone();
    next_manifest.head_log_index = next_log_index;

    ensure_active_segment_metadata(&mut next_manifest);
    state.active_segment_size_bytes = state.active_segment_size_bytes.saturating_add(frame_size);
    let size_bytes = state.active_segment_size_bytes;

    if let Some(meta) = next_manifest
      .segments
      .iter_mut()
      .find(|entry| entry.id == next_manifest.active_segment_id)
    {
      if meta.end_log_index < meta.start_log_index {
        meta.start_log_index = next_log_index;
      }
      meta.end_log_index = next_log_index;
      meta.size_bytes = size_bytes;
    }

    let mut rotated = false;
    if size_bytes >= self.segment_max_bytes {
      rotated = true;
      next_manifest.active_segment_id = next_manifest.active_segment_id.saturating_add(1);
      let start = next_log_index.saturating_add(1);
      next_manifest.segments.push(SegmentMeta {
        id: next_manifest.active_segment_id,
        start_log_index: start,
        end_log_index: start.saturating_sub(1),
        size_bytes: 0,
      });
    }

    let persist_manifest = self.persist_manifest_each_append || rotated || should_refresh;
    if persist_manifest || rotated {
      if let Err(error) = state.log_store.flush() {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(error);
      }
    }
    if persist_manifest {
      if let Err(error) = self.manifest_store.write(&next_manifest) {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(error);
      }
      state.manifest_disk_stamp = read_manifest_disk_stamp(self.manifest_store.path())?;
    }

    let token = CommitToken::new(epoch, next_log_index);
    if rotated {
      state.log_store = SegmentLogStore::open_or_create_append_with_buffer(
        self
          .sidecar_path
          .join(segment_file_name(next_manifest.active_segment_id)),
        self.append_write_buffer_bytes,
      )?;
      state.active_segment_size_bytes = 0;
    }
    state.manifest = next_manifest;
    state.last_token = Some(token);
    state.appends_since_manifest_refresh = state.appends_since_manifest_refresh.saturating_add(1);
    self.append_successes.fetch_add(1, Ordering::Relaxed);
    self
      .epoch_fence
      .store(state.manifest.epoch, Ordering::Release);

    Ok(token)
  }

  fn append_commit_payload_owned_segments(
    &self,
    payload_segments: Vec<Vec<u8>>,
  ) -> Result<CommitToken> {
    self.append_attempts.fetch_add(1, Ordering::Relaxed);

    if let Some(limit) = self.fail_after_append_for_testing {
      let successes = self.append_successes.load(Ordering::Relaxed);
      if successes >= limit {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(KiteError::InvalidReplication(
          "replication append failure injected for testing".to_string(),
        ));
      }
    }

    let _sidecar_guard = self.sidecar_op_lock.lock();
    let mut state = self.state.lock();
    let fenced_epoch = self.epoch_fence.load(Ordering::Acquire);
    if fenced_epoch > state.manifest.epoch {
      state.write_fenced = true;
      self.append_failures.fetch_add(1, Ordering::Relaxed);
      return Err(stale_primary_error());
    }
    if state.write_fenced {
      self.append_failures.fetch_add(1, Ordering::Relaxed);
      return Err(stale_primary_error());
    }
    let should_refresh = state.appends_since_manifest_refresh
      >= self.manifest_refresh_append_interval.saturating_sub(1);
    if should_refresh {
      let epoch_changed = self.refresh_manifest_locked(&mut state)?;
      state.appends_since_manifest_refresh = 0;
      if epoch_changed || state.write_fenced {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(stale_primary_error());
      }
    }

    let epoch = state.manifest.epoch;
    let next_log_index = state.manifest.head_log_index.saturating_add(1);

    let frame_size = match state.log_store.append_payload_owned_segments_with_crc(
      epoch,
      next_log_index,
      payload_segments,
      self.checksum_payload,
    ) {
      Ok(size) => size,
      Err(error) => {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(error);
      }
    };

    if self.durable_append {
      if let Err(error) = state.log_store.sync() {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(error);
      }
    }

    let mut next_manifest = state.manifest.clone();
    next_manifest.head_log_index = next_log_index;

    ensure_active_segment_metadata(&mut next_manifest);
    state.active_segment_size_bytes = state.active_segment_size_bytes.saturating_add(frame_size);
    let size_bytes = state.active_segment_size_bytes;

    if let Some(meta) = next_manifest
      .segments
      .iter_mut()
      .find(|entry| entry.id == next_manifest.active_segment_id)
    {
      if meta.end_log_index < meta.start_log_index {
        meta.start_log_index = next_log_index;
      }
      meta.end_log_index = next_log_index;
      meta.size_bytes = size_bytes;
    }

    let mut rotated = false;
    if size_bytes >= self.segment_max_bytes {
      rotated = true;
      next_manifest.active_segment_id = next_manifest.active_segment_id.saturating_add(1);
      let start = next_log_index.saturating_add(1);
      next_manifest.segments.push(SegmentMeta {
        id: next_manifest.active_segment_id,
        start_log_index: start,
        end_log_index: start.saturating_sub(1),
        size_bytes: 0,
      });
    }

    let persist_manifest = self.persist_manifest_each_append || rotated || should_refresh;
    if persist_manifest || rotated {
      if let Err(error) = state.log_store.flush() {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(error);
      }
    }
    if persist_manifest {
      if let Err(error) = self.manifest_store.write(&next_manifest) {
        self.append_failures.fetch_add(1, Ordering::Relaxed);
        return Err(error);
      }
      state.manifest_disk_stamp = read_manifest_disk_stamp(self.manifest_store.path())?;
    }

    let token = CommitToken::new(epoch, next_log_index);
    if rotated {
      state.log_store = SegmentLogStore::open_or_create_append_with_buffer(
        self
          .sidecar_path
          .join(segment_file_name(next_manifest.active_segment_id)),
        self.append_write_buffer_bytes,
      )?;
      state.active_segment_size_bytes = 0;
    }
    state.manifest = next_manifest;
    state.last_token = Some(token);
    state.appends_since_manifest_refresh = state.appends_since_manifest_refresh.saturating_add(1);
    self.append_successes.fetch_add(1, Ordering::Relaxed);
    self
      .epoch_fence
      .store(state.manifest.epoch, Ordering::Release);

    Ok(token)
  }

  pub fn promote_to_next_epoch(&self) -> Result<u64> {
    let _sidecar_guard = self.sidecar_op_lock.lock();
    let mut state = self.state.lock();
    let epoch_changed = self.refresh_manifest_locked(&mut state)?;
    if epoch_changed || state.write_fenced {
      return Ok(state.manifest.epoch);
    }

    let mut next_manifest = state.manifest.clone();
    next_manifest.epoch = next_manifest.epoch.saturating_add(1);
    next_manifest.active_segment_id = next_manifest.active_segment_id.saturating_add(1);
    next_manifest.segments.push(SegmentMeta {
      id: next_manifest.active_segment_id,
      start_log_index: next_manifest.head_log_index.saturating_add(1),
      end_log_index: next_manifest.head_log_index,
      size_bytes: 0,
    });
    ensure_active_segment_metadata(&mut next_manifest);
    self.manifest_store.write(&next_manifest)?;
    state.manifest_disk_stamp = read_manifest_disk_stamp(self.manifest_store.path())?;

    state.log_store = SegmentLogStore::open_or_create_append_with_buffer(
      self
        .sidecar_path
        .join(segment_file_name(next_manifest.active_segment_id)),
      self.append_write_buffer_bytes,
    )?;
    state.active_segment_size_bytes = 0;
    state.manifest = next_manifest;
    state.last_token = None;
    state.replica_progress.clear();
    clear_replica_progress(&self.sidecar_path)?;
    state.write_fenced = false;
    state.appends_since_manifest_refresh = 0;
    self
      .epoch_fence
      .store(state.manifest.epoch, Ordering::Release);
    Ok(state.manifest.epoch)
  }

  pub fn report_replica_progress(
    &self,
    replica_id: &str,
    epoch: u64,
    applied_log_index: u64,
  ) -> Result<()> {
    let _sidecar_guard = self.sidecar_op_lock.lock();
    let mut state = self.state.lock();
    let epoch_changed = self.refresh_manifest_locked(&mut state)?;
    if epoch_changed || state.write_fenced {
      return Err(stale_primary_error());
    }
    if epoch != state.manifest.epoch {
      return Err(KiteError::InvalidReplication(format!(
        "replica progress epoch mismatch: reported {epoch}, primary epoch {}",
        state.manifest.epoch
      )));
    }

    upsert_replica_progress(&self.sidecar_path, replica_id, epoch, applied_log_index)?;
    state.replica_progress.insert(
      replica_id.to_string(),
      ReplicaProgressEntry {
        epoch,
        applied_log_index,
      },
    );
    Ok(())
  }

  pub fn run_retention(&self) -> Result<PrimaryRetentionOutcome> {
    let _sidecar_guard = self.sidecar_op_lock.lock();
    let mut state = self.state.lock();
    let epoch_changed = self.refresh_manifest_locked(&mut state)?;
    if epoch_changed || state.write_fenced {
      return Err(stale_primary_error());
    }
    self.refresh_replica_progress_locked(&mut state)?;
    state.log_store.flush()?;

    let head = state.manifest.head_log_index;
    let window_floor = head.saturating_sub(self.retention_min_entries);
    let replica_floor = state
      .replica_progress
      .values()
      .filter(|progress| progress.epoch == state.manifest.epoch)
      .map(|progress| progress.applied_log_index.saturating_add(1))
      .min();
    let target_floor = window_floor
      .min(replica_floor.unwrap_or(window_floor))
      .max(state.manifest.retained_floor);

    let mut next_manifest = state.manifest.clone();
    next_manifest.retained_floor = target_floor;
    let retention_cutoff = self
      .retention_min_duration
      .and_then(|duration| SystemTime::now().checked_sub(duration));

    let active_segment_id = next_manifest.active_segment_id;
    let mut pruned_ids = Vec::new();
    let mut retained_segments = Vec::with_capacity(next_manifest.segments.len());
    for segment in &next_manifest.segments {
      if segment.id == active_segment_id {
        retained_segments.push(segment.clone());
        continue;
      }

      let prune_by_index = segment.end_log_index > 0 && segment.end_log_index < target_floor;
      if !prune_by_index {
        retained_segments.push(segment.clone());
        continue;
      }

      if !self.segment_old_enough_for_prune(segment.id, retention_cutoff)? {
        retained_segments.push(segment.clone());
        continue;
      }

      pruned_ids.push(segment.id);
    }
    next_manifest.segments = retained_segments;
    ensure_active_segment_metadata(&mut next_manifest);

    self.manifest_store.write(&next_manifest)?;
    state.manifest_disk_stamp = read_manifest_disk_stamp(self.manifest_store.path())?;
    state.manifest = next_manifest;
    state.appends_since_manifest_refresh = 0;

    for id in &pruned_ids {
      let segment_path = self.sidecar_path.join(segment_file_name(*id));
      if segment_path.exists() {
        std::fs::remove_file(&segment_path)?;
      }
    }

    Ok(PrimaryRetentionOutcome {
      pruned_segments: pruned_ids.len(),
      retained_floor: target_floor,
    })
  }

  pub fn last_token(&self) -> Option<CommitToken> {
    self.state.lock().last_token
  }

  pub fn status(&self) -> PrimaryReplicationStatus {
    let state = self.state.lock();
    let mut replica_lags: Vec<ReplicaLagStatus> = state
      .replica_progress
      .iter()
      .map(|(replica_id, progress)| ReplicaLagStatus {
        replica_id: replica_id.clone(),
        epoch: progress.epoch,
        applied_log_index: progress.applied_log_index,
      })
      .collect();
    replica_lags.sort_by(|left, right| left.replica_id.cmp(&right.replica_id));

    PrimaryReplicationStatus {
      role: ReplicationRole::Primary,
      epoch: state.manifest.epoch,
      head_log_index: state.manifest.head_log_index,
      retained_floor: state.manifest.retained_floor,
      replica_lags,
      sidecar_path: self.sidecar_path.clone(),
      last_token: state.last_token,
      append_attempts: self.append_attempts.load(Ordering::Relaxed),
      append_failures: self.append_failures.load(Ordering::Relaxed),
      append_successes: self.append_successes.load(Ordering::Relaxed),
    }
  }

  pub fn flush_for_transport_export(&self) -> Result<()> {
    let _sidecar_guard = self.sidecar_op_lock.lock();
    let mut state = self.state.lock();
    state.log_store.flush()
  }

  fn refresh_manifest_locked(&self, state: &mut PrimaryReplicationState) -> Result<bool> {
    let disk_stamp = read_manifest_disk_stamp(self.manifest_store.path())?;
    if disk_stamp == state.manifest_disk_stamp {
      return Ok(false);
    }

    let mut persisted = self.manifest_store.read()?;
    ensure_active_segment_metadata(&mut persisted);

    let epoch_changed = persisted.epoch != state.manifest.epoch;
    let active_changed = persisted.active_segment_id != state.manifest.active_segment_id;
    state.manifest_disk_stamp = disk_stamp;

    if epoch_changed {
      state.write_fenced = true;
      state.manifest = persisted;
      self
        .epoch_fence
        .store(state.manifest.epoch, Ordering::Release);
      if active_changed {
        state.log_store = SegmentLogStore::open_or_create_append_with_buffer(
          self
            .sidecar_path
            .join(segment_file_name(state.manifest.active_segment_id)),
          self.append_write_buffer_bytes,
        )?;
        state.active_segment_size_bytes = segment_file_len(
          &self
            .sidecar_path
            .join(segment_file_name(state.manifest.active_segment_id)),
        )?;
      }
      return Ok(true);
    }

    if self.persist_manifest_each_append {
      state.manifest = persisted;
      if active_changed {
        state.log_store = SegmentLogStore::open_or_create_append_with_buffer(
          self
            .sidecar_path
            .join(segment_file_name(state.manifest.active_segment_id)),
          self.append_write_buffer_bytes,
        )?;
        state.active_segment_size_bytes = segment_file_len(
          &self
            .sidecar_path
            .join(segment_file_name(state.manifest.active_segment_id)),
        )?;
      }
      return Ok(false);
    }

    if active_changed {
      state.write_fenced = true;
      state.manifest = persisted;
      self
        .epoch_fence
        .store(state.manifest.epoch, Ordering::Release);
      state.log_store = SegmentLogStore::open_or_create_append_with_buffer(
        self
          .sidecar_path
          .join(segment_file_name(state.manifest.active_segment_id)),
        self.append_write_buffer_bytes,
      )?;
      state.active_segment_size_bytes = segment_file_len(
        &self
          .sidecar_path
          .join(segment_file_name(state.manifest.active_segment_id)),
      )?;
      return Ok(false);
    }

    if persisted.retained_floor > state.manifest.retained_floor {
      state.manifest.retained_floor = persisted.retained_floor;
    }

    Ok(false)
  }

  fn refresh_replica_progress_locked(&self, state: &mut PrimaryReplicationState) -> Result<()> {
    state.replica_progress = load_replica_progress(&self.sidecar_path)?;
    Ok(())
  }

  fn segment_old_enough_for_prune(
    &self,
    segment_id: u64,
    retention_cutoff: Option<SystemTime>,
  ) -> Result<bool> {
    let Some(cutoff) = retention_cutoff else {
      return Ok(true);
    };

    let segment_path = self.sidecar_path.join(segment_file_name(segment_id));
    let metadata = match std::fs::metadata(&segment_path) {
      Ok(metadata) => metadata,
      Err(error) if error.kind() == ErrorKind::NotFound => return Ok(true),
      Err(error) => return Err(error.into()),
    };

    let modified = match metadata.modified() {
      Ok(modified) => modified,
      Err(_) => return Ok(false),
    };

    Ok(modified <= cutoff)
  }
}

pub fn default_replication_sidecar_path(db_path: &Path) -> PathBuf {
  let file_name = db_path
    .file_name()
    .map(|name| format!("{}.replication", name.to_string_lossy()))
    .unwrap_or_else(|| "replication-sidecar".to_string());

  match db_path.parent() {
    Some(parent) => parent.join(file_name),
    None => PathBuf::from(file_name),
  }
}

fn ensure_active_segment_metadata(manifest: &mut ReplicationManifest) {
  let active_id = manifest.active_segment_id;
  if manifest.segments.iter().any(|entry| entry.id == active_id) {
    return;
  }

  let start = manifest.head_log_index.saturating_add(1);
  manifest.segments.push(SegmentMeta {
    id: active_id,
    start_log_index: start,
    end_log_index: start.saturating_sub(1),
    size_bytes: 0,
  });
}

fn segment_file_name(id: u64) -> String {
  format!("segment-{id:020}.rlog")
}

fn reconcile_manifest_head_from_active_segment(
  sidecar_path: &Path,
  manifest: &mut ReplicationManifest,
) -> Result<bool> {
  let segment_path = sidecar_path.join(segment_file_name(manifest.active_segment_id));
  if !segment_path.exists() {
    return Ok(false);
  }

  let (_, _, last_seen) =
    SegmentLogStore::open(&segment_path)?.read_filtered_from_offset(0, |_| false, 0)?;
  let Some((segment_epoch, segment_head_log_index)) = last_seen else {
    return Ok(false);
  };

  if segment_epoch != manifest.epoch || segment_head_log_index <= manifest.head_log_index {
    return Ok(false);
  }

  manifest.head_log_index = segment_head_log_index;
  if let Some(active_segment) = manifest
    .segments
    .iter_mut()
    .find(|entry| entry.id == manifest.active_segment_id)
  {
    if active_segment.end_log_index < segment_head_log_index {
      active_segment.end_log_index = segment_head_log_index;
    }
    if active_segment.start_log_index > active_segment.end_log_index {
      active_segment.start_log_index = active_segment.end_log_index;
    }
    active_segment.size_bytes = segment_file_len(&segment_path)?;
  }

  ensure_active_segment_metadata(manifest);
  Ok(true)
}

fn stale_primary_error() -> KiteError {
  KiteError::InvalidReplication("stale primary is fenced for writes".to_string())
}

fn read_manifest_disk_stamp(path: &Path) -> Result<ManifestDiskStamp> {
  let metadata = std::fs::metadata(path)?;
  let modified_unix_nanos = metadata
    .modified()
    .ok()
    .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
    .map(|value| value.as_nanos());

  Ok(ManifestDiskStamp {
    len: metadata.len(),
    modified_unix_nanos,
  })
}

fn segment_file_len(path: &Path) -> Result<u64> {
  match std::fs::metadata(path) {
    Ok(metadata) => Ok(metadata.len()),
    Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
    Err(error) => Err(error.into()),
  }
}

fn sidecar_operation_lock(sidecar_path: &Path) -> SidecarOpLock {
  let registry = SIDECAR_LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
  let mut registry = registry.lock().expect("sidecar lock registry poisoned");
  registry
    .entry(sidecar_path.to_path_buf())
    .or_insert_with(|| Arc::new(Mutex::new(())))
    .clone()
}

fn acquire_sidecar_primary_lock(sidecar_path: &Path) -> Result<SidecarPrimaryLock> {
  let key = normalize_sidecar_path(sidecar_path);
  let registry = SIDECAR_PRIMARY_LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
  let mut registry = registry
    .lock()
    .map_err(|_| KiteError::LockFailed("primary sidecar lock registry poisoned".to_string()))?;

  if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
    return Ok(existing);
  }

  let lock_path = key.join(PRIMARY_LOCK_FILE_NAME);
  let lock_file = OpenOptions::new()
    .create(true)
    .read(true)
    .write(true)
    .open(&lock_path)?;
  lock_file.try_lock_exclusive().map_err(|error| {
    KiteError::LockFailed(format!(
      "primary sidecar lock is held by another process: {} ({error})",
      lock_path.display()
    ))
  })?;

  let lock = Arc::new(PrimarySidecarProcessLock { _file: lock_file });
  registry.insert(key, Arc::downgrade(&lock));
  Ok(lock)
}

fn sidecar_epoch_fence(sidecar_path: &Path, initial_epoch: u64) -> SidecarEpochFence {
  let key = normalize_sidecar_path(sidecar_path);
  let registry = SIDECAR_EPOCH_FENCES.get_or_init(|| StdMutex::new(HashMap::new()));
  let mut registry = registry
    .lock()
    .expect("sidecar epoch fence registry poisoned");
  let entry = registry
    .entry(key)
    .or_insert_with(|| Arc::downgrade(&Arc::new(AtomicU64::new(initial_epoch))));
  let fence = if let Some(existing) = entry.upgrade() {
    existing
  } else {
    let created = Arc::new(AtomicU64::new(initial_epoch));
    *entry = Arc::downgrade(&created);
    created
  };
  fence.fetch_max(initial_epoch, Ordering::AcqRel);
  fence
}

fn normalize_sidecar_path(path: &Path) -> PathBuf {
  std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}
