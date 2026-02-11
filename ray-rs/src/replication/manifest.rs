//! Replication manifest sidecar storage.

use crate::error::{KiteError, Result};
use crate::util::crc::crc32c;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub const MANIFEST_ENVELOPE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentMeta {
  pub id: u64,
  pub start_log_index: u64,
  pub end_log_index: u64,
  pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationManifest {
  pub version: u32,
  pub epoch: u64,
  pub head_log_index: u64,
  pub retained_floor: u64,
  pub active_segment_id: u64,
  pub segments: Vec<SegmentMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ManifestEnvelope {
  pub version: u32,
  pub payload_crc32: u32,
  pub manifest: ReplicationManifest,
}

#[derive(Debug, Clone)]
pub struct ManifestStore {
  path: PathBuf,
}

impl ManifestStore {
  pub fn new(path: impl AsRef<Path>) -> Self {
    Self {
      path: path.as_ref().to_path_buf(),
    }
  }

  pub fn path(&self) -> &Path {
    &self.path
  }

  pub fn temp_path(&self) -> PathBuf {
    match self
      .path
      .extension()
      .and_then(|extension| extension.to_str())
    {
      Some(extension) => self.path.with_extension(format!("{extension}.tmp")),
      None => self.path.with_extension("tmp"),
    }
  }

  pub fn read(&self) -> Result<ReplicationManifest> {
    let bytes = fs::read(&self.path)?;
    decode_manifest_bytes(&bytes)
  }

  pub fn write(&self, manifest: &ReplicationManifest) -> Result<()> {
    if let Some(parent) = self.path.parent() {
      fs::create_dir_all(parent)?;
    }

    let temp_path = self.temp_path();
    let bytes = encode_manifest_bytes(manifest)?;

    let mut temp_file = OpenOptions::new()
      .create(true)
      .truncate(true)
      .write(true)
      .open(&temp_path)?;

    temp_file.write_all(&bytes)?;
    temp_file.sync_all()?;

    fs::rename(&temp_path, &self.path)?;
    sync_parent_dir(self.path.parent())?;

    Ok(())
  }
}

fn encode_manifest_bytes(manifest: &ReplicationManifest) -> Result<Vec<u8>> {
  let payload = serde_json::to_vec(manifest).map_err(|error| {
    KiteError::Serialization(format!("encode replication manifest payload: {error}"))
  })?;

  let envelope = ManifestEnvelope {
    version: MANIFEST_ENVELOPE_VERSION,
    payload_crc32: crc32c(&payload),
    manifest: manifest.clone(),
  };

  serde_json::to_vec(&envelope).map_err(|error| {
    KiteError::Serialization(format!("encode replication manifest envelope: {error}"))
  })
}

fn decode_manifest_bytes(bytes: &[u8]) -> Result<ReplicationManifest> {
  let envelope: ManifestEnvelope = serde_json::from_slice(bytes).map_err(|error| {
    KiteError::Serialization(format!("decode replication manifest envelope: {error}"))
  })?;

  if envelope.version != MANIFEST_ENVELOPE_VERSION {
    return Err(KiteError::VersionMismatch {
      required: envelope.version,
      current: MANIFEST_ENVELOPE_VERSION,
    });
  }

  let payload = serde_json::to_vec(&envelope.manifest).map_err(|error| {
    KiteError::Serialization(format!("encode replication manifest payload: {error}"))
  })?;

  let computed = crc32c(&payload);
  if computed != envelope.payload_crc32 {
    return Err(KiteError::CrcMismatch {
      stored: envelope.payload_crc32,
      computed,
    });
  }

  Ok(envelope.manifest)
}

fn sync_parent_dir(parent: Option<&Path>) -> Result<()> {
  #[cfg(unix)]
  {
    if let Some(parent) = parent {
      let directory = File::open(parent)?;
      directory.sync_all()?;
    }
  }

  #[cfg(not(unix))]
  {
    let _ = parent;
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::{ManifestEnvelope, ManifestStore, ReplicationManifest, SegmentMeta};

  fn sample_manifest() -> ReplicationManifest {
    ReplicationManifest {
      version: 1,
      epoch: 7,
      head_log_index: 99,
      retained_floor: 42,
      active_segment_id: 3,
      segments: vec![
        SegmentMeta {
          id: 2,
          start_log_index: 1,
          end_log_index: 64,
          size_bytes: 1024,
        },
        SegmentMeta {
          id: 3,
          start_log_index: 65,
          end_log_index: 99,
          size_bytes: 512,
        },
      ],
    }
  }

  #[test]
  fn write_then_read_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("manifest.json");
    let store = ManifestStore::new(path);

    let manifest = sample_manifest();
    store.write(&manifest).expect("write");

    let loaded = store.read().expect("read");
    assert_eq!(loaded, manifest);
  }

  #[test]
  fn checksum_mismatch_fails_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("manifest.json");
    let store = ManifestStore::new(&path);

    let manifest = sample_manifest();
    store.write(&manifest).expect("write");

    let mut envelope: ManifestEnvelope =
      serde_json::from_slice(&std::fs::read(&path).expect("read bytes")).expect("parse envelope");
    envelope.payload_crc32 ^= 0xFF;
    std::fs::write(
      &path,
      serde_json::to_vec(&envelope).expect("encode envelope"),
    )
    .expect("write envelope");

    assert!(store.read().is_err());
  }
}
