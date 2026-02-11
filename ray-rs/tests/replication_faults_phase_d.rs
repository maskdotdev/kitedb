use kitedb::core::single_file::{close_single_file, open_single_file, SingleFileOpenOptions};
use kitedb::replication::types::ReplicationRole;

fn open_primary(
  path: &std::path::Path,
  sidecar: &std::path::Path,
) -> kitedb::Result<kitedb::core::single_file::SingleFileDB> {
  open_single_file(
    path,
    SingleFileOpenOptions::new()
      .replication_role(ReplicationRole::Primary)
      .replication_sidecar_path(sidecar)
      .replication_segment_max_bytes(1024 * 1024)
      .replication_retention_min_entries(128),
  )
}

fn open_primary_with_segment_limit(
  path: &std::path::Path,
  sidecar: &std::path::Path,
  segment_max_bytes: u64,
) -> kitedb::Result<kitedb::core::single_file::SingleFileDB> {
  open_single_file(
    path,
    SingleFileOpenOptions::new()
      .replication_role(ReplicationRole::Primary)
      .replication_sidecar_path(sidecar)
      .replication_segment_max_bytes(segment_max_bytes)
      .replication_retention_min_entries(128),
  )
}

fn open_replica(
  replica_path: &std::path::Path,
  source_db_path: &std::path::Path,
  local_sidecar: &std::path::Path,
  source_sidecar: &std::path::Path,
) -> kitedb::Result<kitedb::core::single_file::SingleFileDB> {
  open_single_file(
    replica_path,
    SingleFileOpenOptions::new()
      .replication_role(ReplicationRole::Replica)
      .replication_sidecar_path(local_sidecar)
      .replication_source_db_path(source_db_path)
      .replication_source_sidecar_path(source_sidecar),
  )
}

fn active_segment_path(sidecar: &std::path::Path) -> std::path::PathBuf {
  sidecar.join("segment-00000000000000000001.rlog")
}

#[test]
fn corrupt_segment_sets_replica_last_error() {
  let dir = tempfile::tempdir().expect("tempdir");
  let primary_path = dir.path().join("fault-corrupt-primary.kitedb");
  let primary_sidecar = dir.path().join("fault-corrupt-primary.sidecar");
  let replica_path = dir.path().join("fault-corrupt-replica.kitedb");
  let replica_sidecar = dir.path().join("fault-corrupt-replica.sidecar");

  let primary = open_primary(&primary_path, &primary_sidecar).expect("open primary");
  primary.begin(false).expect("begin base");
  primary.create_node(Some("base")).expect("create base");
  primary
    .commit_with_token()
    .expect("commit base")
    .expect("token base");

  let replica = open_replica(
    &replica_path,
    &primary_path,
    &replica_sidecar,
    &primary_sidecar,
  )
  .expect("open replica");
  replica
    .replica_bootstrap_from_snapshot()
    .expect("bootstrap snapshot");

  primary.begin(false).expect("begin c1");
  primary.create_node(Some("c1")).expect("create c1");
  primary
    .commit_with_token()
    .expect("commit c1")
    .expect("token c1");
  close_single_file(primary).expect("close primary");

  let segment_path = active_segment_path(&primary_sidecar);
  let mut bytes = std::fs::read(&segment_path).expect("read segment");
  bytes[31] ^= 0xFF;
  std::fs::write(&segment_path, &bytes).expect("write corrupted segment");

  let err = replica
    .replica_catch_up_once(32)
    .expect_err("corrupted segment must fail catch-up");
  assert!(
    err.to_string().contains("CRC mismatch"),
    "unexpected corruption error: {err}"
  );
  let status = replica.replica_replication_status().expect("status");
  assert!(status.last_error.is_some(), "last_error must be persisted");
  assert!(!status.needs_reseed);

  close_single_file(replica).expect("close replica");
}

#[test]
fn truncated_segment_sets_replica_last_error() {
  let dir = tempfile::tempdir().expect("tempdir");
  let primary_path = dir.path().join("fault-truncated-primary.kitedb");
  let primary_sidecar = dir.path().join("fault-truncated-primary.sidecar");
  let replica_path = dir.path().join("fault-truncated-replica.kitedb");
  let replica_sidecar = dir.path().join("fault-truncated-replica.sidecar");

  let primary = open_primary(&primary_path, &primary_sidecar).expect("open primary");
  primary.begin(false).expect("begin base");
  primary.create_node(Some("base")).expect("create base");
  primary
    .commit_with_token()
    .expect("commit base")
    .expect("token base");

  let replica = open_replica(
    &replica_path,
    &primary_path,
    &replica_sidecar,
    &primary_sidecar,
  )
  .expect("open replica");
  replica
    .replica_bootstrap_from_snapshot()
    .expect("bootstrap snapshot");

  primary.begin(false).expect("begin c1");
  primary.create_node(Some("c1")).expect("create c1");
  primary
    .commit_with_token()
    .expect("commit c1")
    .expect("token c1");
  close_single_file(primary).expect("close primary");

  let segment_path = active_segment_path(&primary_sidecar);
  let mut bytes = std::fs::read(&segment_path).expect("read segment");
  bytes.truncate(bytes.len().saturating_sub(1));
  std::fs::write(&segment_path, &bytes).expect("write truncated segment");

  let err = replica
    .replica_catch_up_once(32)
    .expect_err("truncated segment must fail catch-up");
  assert!(
    err.to_string().contains("truncated replication segment"),
    "unexpected truncation error: {err}"
  );
  let status = replica.replica_replication_status().expect("status");
  assert!(status.last_error.is_some(), "last_error must be persisted");
  assert!(!status.needs_reseed);

  close_single_file(replica).expect("close replica");
}

#[test]
fn obsolete_corrupt_segment_does_not_break_incremental_catch_up() {
  let dir = tempfile::tempdir().expect("tempdir");
  let primary_path = dir.path().join("fault-obsolete-corrupt-primary.kitedb");
  let primary_sidecar = dir.path().join("fault-obsolete-corrupt-primary.sidecar");
  let replica_path = dir.path().join("fault-obsolete-corrupt-replica.kitedb");
  let replica_sidecar = dir.path().join("fault-obsolete-corrupt-replica.sidecar");

  let primary =
    open_primary_with_segment_limit(&primary_path, &primary_sidecar, 1).expect("open primary");
  primary.begin(false).expect("begin base");
  primary.create_node(Some("base")).expect("create base");
  primary
    .commit_with_token()
    .expect("commit base")
    .expect("token base");

  let replica = open_replica(
    &replica_path,
    &primary_path,
    &replica_sidecar,
    &primary_sidecar,
  )
  .expect("open replica");
  replica
    .replica_bootstrap_from_snapshot()
    .expect("bootstrap snapshot");

  for i in 0..5 {
    primary.begin(false).expect("begin seed");
    primary
      .create_node(Some(&format!("seed-{i}")))
      .expect("create seed");
    primary
      .commit_with_token()
      .expect("commit seed")
      .expect("token seed");
  }

  let initial = replica
    .replica_catch_up_once(128)
    .expect("initial catch-up");
  assert!(initial > 0, "replica must establish applied cursor");

  let oldest_segment = active_segment_path(&primary_sidecar);
  let mut bytes = std::fs::read(&oldest_segment).expect("read oldest segment");
  bytes[0] ^= 0xFF;
  std::fs::write(&oldest_segment, &bytes).expect("corrupt obsolete segment");

  primary.begin(false).expect("begin tail");
  primary.create_node(Some("tail")).expect("create tail");
  primary
    .commit_with_token()
    .expect("commit tail")
    .expect("token tail");

  let pulled = replica
    .replica_catch_up_once(8)
    .expect("catch-up should ignore obsolete corruption");
  assert!(pulled > 0, "replica must still pull newest frames");
  assert!(
    !replica
      .replica_replication_status()
      .expect("replica status")
      .needs_reseed
  );

  close_single_file(replica).expect("close replica");
  close_single_file(primary).expect("close primary");
}

#[cfg(unix)]
#[test]
fn cursor_persist_failure_does_not_advance_in_memory_position() {
  use std::os::unix::fs::PermissionsExt;

  let dir = tempfile::tempdir().expect("tempdir");
  let primary_path = dir.path().join("fault-cursor-persist-primary.kitedb");
  let primary_sidecar = dir.path().join("fault-cursor-persist-primary.sidecar");
  let replica_path = dir.path().join("fault-cursor-persist-replica.kitedb");
  let replica_sidecar = dir.path().join("fault-cursor-persist-replica.sidecar");

  let primary = open_primary(&primary_path, &primary_sidecar).expect("open primary");
  primary.begin(false).expect("begin base");
  primary.create_node(Some("base")).expect("create base");
  primary
    .commit_with_token()
    .expect("commit base")
    .expect("token base");

  let replica = open_replica(
    &replica_path,
    &primary_path,
    &replica_sidecar,
    &primary_sidecar,
  )
  .expect("open replica");
  replica
    .replica_bootstrap_from_snapshot()
    .expect("bootstrap snapshot");

  primary.begin(false).expect("begin c1");
  primary.create_node(Some("c1")).expect("create c1");
  primary
    .commit_with_token()
    .expect("commit c1")
    .expect("token c1");

  let before = replica
    .replica_replication_status()
    .expect("status before persist failure");

  let original_mode = std::fs::metadata(&replica_sidecar)
    .expect("replica sidecar metadata")
    .permissions()
    .mode();
  std::fs::set_permissions(&replica_sidecar, std::fs::Permissions::from_mode(0o555))
    .expect("set read-only sidecar permissions");

  let err = replica
    .replica_catch_up_once(32)
    .expect_err("cursor persist failure must fail catch-up");
  assert!(
    err.to_string().contains("cursor persist failed"),
    "unexpected cursor persist failure: {err}"
  );

  let after = replica
    .replica_replication_status()
    .expect("status after persist failure");
  assert_eq!(
    after.applied_log_index, before.applied_log_index,
    "in-memory applied log index must not advance when cursor persistence fails"
  );
  assert_eq!(
    after.applied_epoch, before.applied_epoch,
    "in-memory applied epoch must not advance when cursor persistence fails"
  );

  std::fs::set_permissions(
    &replica_sidecar,
    std::fs::Permissions::from_mode(original_mode),
  )
  .expect("restore sidecar permissions");

  close_single_file(replica).expect("close replica");
  close_single_file(primary).expect("close primary");
}
