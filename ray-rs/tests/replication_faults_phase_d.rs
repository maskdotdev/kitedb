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
