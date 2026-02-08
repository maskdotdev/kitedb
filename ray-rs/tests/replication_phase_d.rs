use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use kitedb::core::single_file::{close_single_file, open_single_file, SingleFileOpenOptions};
use kitedb::replication::types::ReplicationRole;

fn open_primary(
  path: &std::path::Path,
  sidecar: &std::path::Path,
  segment_max_bytes: u64,
  retention_min_entries: u64,
) -> kitedb::Result<kitedb::core::single_file::SingleFileDB> {
  open_single_file(
    path,
    SingleFileOpenOptions::new()
      .replication_role(ReplicationRole::Primary)
      .replication_sidecar_path(sidecar)
      .replication_segment_max_bytes(segment_max_bytes)
      .replication_retention_min_entries(retention_min_entries),
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

#[test]
fn promotion_increments_epoch_and_fences_stale_primary_writes() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("phase-d-promote.kitedb");
  let sidecar = dir.path().join("phase-d-promote.sidecar");

  let primary_a = open_primary(&db_path, &sidecar, 256, 4).expect("open primary a");
  let primary_b = open_primary(&db_path, &sidecar, 256, 4).expect("open primary b");

  primary_a.begin(false).expect("begin a");
  primary_a.create_node(Some("a0")).expect("create a0");
  let t0 = primary_a
    .commit_with_token()
    .expect("commit a0")
    .expect("token a0");
  assert_eq!(t0.epoch, 1);

  let new_epoch = primary_b.primary_promote_to_next_epoch().expect("promote");
  assert_eq!(new_epoch, 2);

  primary_b.begin(false).expect("begin b");
  primary_b.create_node(Some("b0")).expect("create b0");
  let t1 = primary_b
    .commit_with_token()
    .expect("commit b0")
    .expect("token b0");
  assert_eq!(t1.epoch, 2);

  primary_a.begin(false).expect("begin stale");
  primary_a.create_node(Some("stale")).expect("create stale");
  let err = primary_a
    .commit_with_token()
    .expect_err("stale primary commit must fail");
  assert!(
    err.to_string().contains("stale primary"),
    "unexpected stale commit error: {err}"
  );

  close_single_file(primary_b).expect("close b");
  close_single_file(primary_a).expect("close a");
}

#[test]
fn retention_respects_active_replica_cursor_and_minimum_window() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("phase-d-retention.kitedb");
  let sidecar = dir.path().join("phase-d-retention.sidecar");

  let primary = open_primary(&db_path, &sidecar, 1, 2).expect("open primary");

  for i in 0..6 {
    primary.begin(false).expect("begin");
    primary
      .create_node(Some(&format!("n-{i}")))
      .expect("create");
    let _ = primary.commit_with_token().expect("commit").expect("token");
  }

  primary
    .primary_report_replica_progress("replica-a", 1, 2)
    .expect("report cursor");

  let prune = primary.primary_run_retention().expect("run retention");
  assert!(prune.pruned_segments > 0);

  let status = primary.primary_replication_status().expect("status");
  assert_eq!(status.retained_floor, 3);
  assert!(status
    .replica_lags
    .iter()
    .any(|lag| lag.replica_id == "replica-a" && lag.applied_log_index == 2));

  close_single_file(primary).expect("close primary");
}

#[test]
fn missing_segment_marks_replica_needs_reseed() {
  let dir = tempfile::tempdir().expect("tempdir");
  let primary_path = dir.path().join("phase-d-missing-primary.kitedb");
  let primary_sidecar = dir.path().join("phase-d-missing-primary.sidecar");
  let replica_path = dir.path().join("phase-d-missing-replica.kitedb");
  let replica_sidecar = dir.path().join("phase-d-missing-replica.sidecar");

  let primary = open_primary(&primary_path, &primary_sidecar, 1, 2).expect("open primary");

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

  for i in 0..4 {
    primary.begin(false).expect("begin");
    primary
      .create_node(Some(&format!("m-{i}")))
      .expect("create");
    primary.commit_with_token().expect("commit").expect("token");
  }

  primary
    .primary_report_replica_progress("replica-m", 1, 1)
    .expect("report lagging cursor");
  let _ = primary.primary_run_retention().expect("run retention");

  let err = replica
    .replica_catch_up_once(32)
    .expect_err("replica should require reseed");
  assert!(err.to_string().contains("reseed"));

  let status = replica
    .replica_replication_status()
    .expect("replica status");
  assert!(status.needs_reseed);

  close_single_file(replica).expect("close replica");
  close_single_file(primary).expect("close primary");
}

#[test]
fn lagging_replica_reseed_recovers_after_retention_gap() {
  let dir = tempfile::tempdir().expect("tempdir");
  let primary_path = dir.path().join("phase-d-reseed-primary.kitedb");
  let primary_sidecar = dir.path().join("phase-d-reseed-primary.sidecar");
  let replica_path = dir.path().join("phase-d-reseed-replica.kitedb");
  let replica_sidecar = dir.path().join("phase-d-reseed-replica.sidecar");

  let primary = open_primary(&primary_path, &primary_sidecar, 1, 2).expect("open primary");

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
    primary.begin(false).expect("begin");
    primary
      .create_node(Some(&format!("r-{i}")))
      .expect("create");
    primary.commit_with_token().expect("commit").expect("token");
  }

  primary
    .primary_report_replica_progress("replica-r", 1, 1)
    .expect("report lagging cursor");
  let _ = primary.primary_run_retention().expect("run retention");

  let _ = replica
    .replica_catch_up_once(32)
    .expect_err("must need reseed");
  assert!(
    replica
      .replica_replication_status()
      .expect("status")
      .needs_reseed
  );

  replica.replica_reseed_from_snapshot().expect("reseed");
  assert!(
    !replica
      .replica_replication_status()
      .expect("status post reseed")
      .needs_reseed
  );
  assert_eq!(replica.count_nodes(), primary.count_nodes());

  close_single_file(replica).expect("close replica");
  close_single_file(primary).expect("close primary");
}

#[test]
fn promotion_race_rejects_split_brain_writes() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("phase-d-race.kitedb");
  let sidecar = dir.path().join("phase-d-race.sidecar");

  let left = Arc::new(open_primary(&db_path, &sidecar, 128, 8).expect("open left"));
  let right = Arc::new(open_primary(&db_path, &sidecar, 128, 8).expect("open right"));

  let l = Arc::clone(&left);
  let h1 = std::thread::spawn(move || {
    let promote = l.primary_promote_to_next_epoch();
    l.begin(false).expect("left begin");
    l.create_node(Some("left")).expect("left create");
    let commit = l.commit_with_token();
    (promote, commit)
  });

  let r = Arc::clone(&right);
  let h2 = std::thread::spawn(move || {
    let promote = r.primary_promote_to_next_epoch();
    r.begin(false).expect("right begin");
    r.create_node(Some("right")).expect("right create");
    let commit = r.commit_with_token();
    (promote, commit)
  });

  let (left_promote, left_result) = h1.join().expect("left join");
  let (right_promote, right_result) = h2.join().expect("right join");
  assert!(left_promote.is_ok());
  assert!(right_promote.is_ok());

  let left_ok = left_result.as_ref().is_ok_and(|token| token.is_some());
  let right_ok = right_result.as_ref().is_ok_and(|token| token.is_some());
  assert!(
    left_ok ^ right_ok,
    "exactly one writer should succeed after race"
  );

  let left = Arc::into_inner(left).expect("left unique");
  let right = Arc::into_inner(right).expect("right unique");
  close_single_file(left).expect("close left");
  close_single_file(right).expect("close right");
}

#[test]
fn retention_time_window_keeps_recent_segments() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("phase-d-retention-window.kitedb");
  let sidecar = dir.path().join("phase-d-retention-window.sidecar");

  let primary = open_single_file(
    &db_path,
    SingleFileOpenOptions::new()
      .replication_role(ReplicationRole::Primary)
      .replication_sidecar_path(&sidecar)
      .replication_segment_max_bytes(1)
      .replication_retention_min_entries(0)
      .replication_retention_min_ms(60_000),
  )
  .expect("open primary");

  for i in 0..6 {
    primary.begin(false).expect("begin");
    primary
      .create_node(Some(&format!("w-{i}")))
      .expect("create");
    primary.commit_with_token().expect("commit").expect("token");
  }

  let segments_before = std::fs::read_dir(&sidecar)
    .expect("list sidecar")
    .filter_map(|entry| entry.ok())
    .filter(|entry| entry.file_name().to_string_lossy().starts_with("segment-"))
    .count();
  assert!(
    segments_before > 1,
    "expected multiple segments for retention"
  );

  let prune = primary.primary_run_retention().expect("run retention");
  assert_eq!(prune.pruned_segments, 0);

  // Ensure no filesystem-timestamp race with segment creation.
  std::thread::sleep(Duration::from_millis(5));

  let segments_after = std::fs::read_dir(&sidecar)
    .expect("list sidecar after retention")
    .filter_map(|entry| entry.ok())
    .filter(|entry| entry.file_name().to_string_lossy().starts_with("segment-"))
    .count();
  assert_eq!(segments_after, segments_before);

  close_single_file(primary).expect("close primary");
}

#[test]
fn replica_open_requires_source_db_path() {
  let dir = tempfile::tempdir().expect("tempdir");
  let replica_path = dir.path().join("phase-d-misconfig-no-source.kitedb");
  let replica_sidecar = dir.path().join("phase-d-misconfig-no-source.sidecar");

  let err = open_single_file(
    &replica_path,
    SingleFileOpenOptions::new()
      .replication_role(ReplicationRole::Replica)
      .replication_sidecar_path(&replica_sidecar),
  )
  .err()
  .expect("replica open without source db path must fail");

  assert!(
    err.to_string().contains("source db path"),
    "unexpected error: {err}"
  );
}

#[test]
fn replica_open_rejects_source_sidecar_equal_local_sidecar() {
  let dir = tempfile::tempdir().expect("tempdir");
  let primary_path = dir.path().join("phase-d-misconfig-primary.kitedb");
  let primary_sidecar = dir.path().join("phase-d-misconfig-primary.sidecar");
  let replica_path = dir.path().join("phase-d-misconfig-replica.kitedb");

  let primary = open_primary(&primary_path, &primary_sidecar, 128, 8).expect("open primary");
  primary.begin(false).expect("begin primary");
  primary.create_node(Some("seed")).expect("create seed");
  primary.commit_with_token().expect("commit primary");

  let err = open_single_file(
    &replica_path,
    SingleFileOpenOptions::new()
      .replication_role(ReplicationRole::Replica)
      .replication_sidecar_path(&primary_sidecar)
      .replication_source_db_path(&primary_path)
      .replication_source_sidecar_path(&primary_sidecar),
  )
  .err()
  .expect("replica local/source sidecar collision must fail");

  assert!(
    err.to_string().contains("source sidecar path must differ"),
    "unexpected error: {err}"
  );

  close_single_file(primary).expect("close primary");
}

#[test]
fn primary_snapshot_transport_export_includes_metadata_and_optional_data() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("phase-d-transport-snapshot.kitedb");
  let sidecar = dir.path().join("phase-d-transport-snapshot.sidecar");
  let primary = open_primary(&db_path, &sidecar, 128, 8).expect("open primary");

  primary.begin(false).expect("begin");
  primary.create_node(Some("snap-1")).expect("create");
  primary.commit_with_token().expect("commit");

  let without_data = primary
    .primary_export_snapshot_transport_json(false)
    .expect("snapshot transport export");
  let without_data_json: serde_json::Value =
    serde_json::from_str(&without_data).expect("parse snapshot export");
  assert_eq!(without_data_json["format"], "single-file-db-copy");
  assert_eq!(without_data_json["epoch"], 1);
  assert_eq!(without_data_json["data_base64"], serde_json::Value::Null);
  assert!(without_data_json["checksum_crc32c"]
    .as_str()
    .map(|value| !value.is_empty())
    .unwrap_or(false));

  let with_data = primary
    .primary_export_snapshot_transport_json(true)
    .expect("snapshot export with data");
  let with_data_json: serde_json::Value =
    serde_json::from_str(&with_data).expect("parse snapshot export with data");
  let encoded = with_data_json["data_base64"]
    .as_str()
    .expect("data_base64 must be present");
  let decoded = BASE64_STANDARD
    .decode(encoded)
    .expect("decode snapshot base64");
  assert_eq!(
    decoded.len() as u64,
    with_data_json["byte_length"]
      .as_u64()
      .expect("byte_length must be u64")
  );

  close_single_file(primary).expect("close primary");
}

#[test]
fn primary_log_transport_export_pages_by_cursor() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("phase-d-transport-log.kitedb");
  let sidecar = dir.path().join("phase-d-transport-log.sidecar");
  let primary = open_primary(&db_path, &sidecar, 1, 2).expect("open primary");

  for i in 0..5 {
    primary.begin(false).expect("begin");
    primary
      .create_node(Some(&format!("transport-{i}")))
      .expect("create");
    primary.commit_with_token().expect("commit");
  }

  let first = primary
    .primary_export_log_transport_json(None, 2, 1024 * 1024, true)
    .expect("first log export");
  let first_json: serde_json::Value = serde_json::from_str(&first).expect("parse first page");
  assert_eq!(first_json["frame_count"], 2);
  assert_eq!(first_json["eof"], false);
  assert!(first_json["frames"]
    .as_array()
    .expect("frames array")
    .iter()
    .all(|frame| frame["payload_base64"].as_str().is_some()));

  let cursor = first_json["next_cursor"]
    .as_str()
    .expect("next_cursor")
    .to_string();
  let second = primary
    .primary_export_log_transport_json(Some(&cursor), 4, 1024 * 1024, false)
    .expect("second log export");
  let second_json: serde_json::Value = serde_json::from_str(&second).expect("parse second page");
  assert!(second_json["frame_count"].as_u64().unwrap_or_default() > 0);
  assert!(second_json["frames"]
    .as_array()
    .expect("frames array")
    .iter()
    .all(|frame| frame["payload_base64"].is_null()));

  close_single_file(primary).expect("close primary");
}
