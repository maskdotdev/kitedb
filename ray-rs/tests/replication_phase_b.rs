use std::collections::HashSet;
use std::env;
use std::sync::{Arc, Barrier};

use kitedb::core::single_file::{close_single_file, open_single_file, SingleFileOpenOptions};
use kitedb::replication::primary::default_replication_sidecar_path;
use kitedb::replication::types::CommitToken;
use kitedb::replication::types::ReplicationRole;

const CRASH_BOUNDARY_CHILD_ENV: &str = "RAYDB_CRASH_BOUNDARY_CHILD";
const CRASH_BOUNDARY_DB_PATH_ENV: &str = "RAYDB_CRASH_BOUNDARY_DB_PATH";
const CRASH_BOUNDARY_TOKEN_PATH_ENV: &str = "RAYDB_CRASH_BOUNDARY_TOKEN_PATH";

#[test]
fn crash_boundary_child_process_helper() {
  if env::var_os(CRASH_BOUNDARY_CHILD_ENV).is_none() {
    return;
  }

  let db_path =
    std::path::PathBuf::from(env::var(CRASH_BOUNDARY_DB_PATH_ENV).expect("child db path env"));
  let token_path = std::path::PathBuf::from(
    env::var(CRASH_BOUNDARY_TOKEN_PATH_ENV).expect("child token path env"),
  );

  let primary = open_single_file(
    &db_path,
    SingleFileOpenOptions::new().replication_role(ReplicationRole::Primary),
  )
  .expect("open child primary");
  primary.begin(false).expect("begin child tx");
  primary
    .create_node(Some("crash-boundary"))
    .expect("create crash-boundary node");
  let token = primary
    .commit_with_token()
    .expect("commit child tx")
    .expect("commit token");
  std::fs::write(&token_path, token.to_string()).expect("persist emitted token");
  std::process::abort();
}

#[test]
fn commit_returns_monotonic_token_on_primary() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("phase-b-primary.kitedb");

  let db = open_single_file(
    &db_path,
    SingleFileOpenOptions::new().replication_role(ReplicationRole::Primary),
  )
  .expect("open db");

  let mut seen = Vec::new();
  for i in 0..4 {
    db.begin(false).expect("begin");
    db.create_node(Some(&format!("n-{i}")))
      .expect("create node");
    let token = db
      .commit_with_token()
      .expect("commit")
      .expect("primary token");
    seen.push(token);
  }

  assert!(seen.windows(2).all(|window| window[0] < window[1]));

  let status = db.primary_replication_status().expect("replication status");
  assert_eq!(status.head_log_index, 4);
  assert_eq!(status.last_token, seen.last().copied());

  close_single_file(db).expect("close db");
}

#[test]
fn replication_disabled_mode_has_no_sidecar_activity() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("phase-b-disabled.kitedb");

  let db = open_single_file(&db_path, SingleFileOpenOptions::new()).expect("open db");
  db.begin(false).expect("begin");
  db.create_node(Some("plain")).expect("create node");
  let token = db.commit_with_token().expect("commit");
  assert!(token.is_none());

  close_single_file(db).expect("close db");

  let default_sidecar = default_replication_sidecar_path(&db_path);
  assert!(
    !default_sidecar.exists(),
    "disabled mode must not create sidecar: {}",
    default_sidecar.display()
  );
}

#[test]
fn sidecar_append_failure_causes_commit_failure_without_token() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("phase-b-failure.kitedb");

  let db = open_single_file(
    &db_path,
    SingleFileOpenOptions::new()
      .replication_role(ReplicationRole::Primary)
      .replication_fail_after_append_for_testing(0),
  )
  .expect("open db");

  db.begin(false).expect("begin");
  db.create_node(Some("boom")).expect("create node");
  let err = db.commit_with_token().expect_err("commit should fail");
  assert!(
    err.to_string().contains("replication append"),
    "unexpected error: {err}"
  );

  let status = db.primary_replication_status().expect("status");
  assert_eq!(status.head_log_index, 0);
  assert_eq!(status.append_failures, 1);
  assert!(db.last_commit_token().is_none());

  close_single_file(db).expect("close db");
}

#[test]
fn concurrent_writers_have_contiguous_token_order() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("phase-b-concurrent.kitedb");

  let db = Arc::new(
    open_single_file(
      &db_path,
      SingleFileOpenOptions::new().replication_role(ReplicationRole::Primary),
    )
    .expect("open db"),
  );

  let threads = 8usize;
  let barrier = Arc::new(Barrier::new(threads));
  let mut handles = Vec::with_capacity(threads);

  for i in 0..threads {
    let db = Arc::clone(&db);
    let barrier = Arc::clone(&barrier);
    handles.push(std::thread::spawn(move || {
      barrier.wait();
      db.begin(false).expect("begin");
      db.create_node(Some(&format!("t-{i}"))).expect("create");
      db.commit_with_token()
        .expect("commit")
        .expect("primary token")
    }));
  }

  let mut tokens = Vec::new();
  for handle in handles {
    tokens.push(handle.join().expect("join"));
  }

  let mut indices: Vec<u64> = tokens.iter().map(|token| token.log_index).collect();
  indices.sort_unstable();
  assert_eq!(indices, (1_u64..=threads as u64).collect::<Vec<_>>());

  let unique: HashSet<u64> = tokens.iter().map(|token| token.log_index).collect();
  assert_eq!(unique.len(), threads);

  let status = db.primary_replication_status().expect("status");
  assert_eq!(status.head_log_index, threads as u64);

  let db = Arc::into_inner(db).expect("sole owner");
  close_single_file(db).expect("close db");
}

#[test]
fn crash_after_commit_token_return_keeps_token_durable_on_reopen() {
  let dir = tempfile::tempdir().expect("tempdir");
  let db_path = dir.path().join("phase-b-crash-boundary.kitedb");
  let token_path = dir.path().join("phase-b-crash-boundary.token");

  let status = std::process::Command::new(std::env::current_exe().expect("current test binary"))
    .arg("--test-threads=1")
    .arg("--exact")
    .arg("crash_boundary_child_process_helper")
    .arg("--nocapture")
    .env(CRASH_BOUNDARY_CHILD_ENV, "1")
    .env(CRASH_BOUNDARY_DB_PATH_ENV, db_path.as_os_str())
    .env(CRASH_BOUNDARY_TOKEN_PATH_ENV, token_path.as_os_str())
    .status()
    .expect("spawn crash-boundary child");
  assert!(
    !status.success(),
    "child helper should crash to emulate abrupt process termination"
  );

  let token_raw = std::fs::read_to_string(&token_path).expect("read emitted token");
  let emitted_token = token_raw.parse::<CommitToken>().expect("parse token");

  let reopened = open_single_file(
    &db_path,
    SingleFileOpenOptions::new().replication_role(ReplicationRole::Primary),
  )
  .expect("reopen primary after crash");
  let status = reopened
    .primary_replication_status()
    .expect("primary status");
  assert!(
    status.head_log_index >= emitted_token.log_index,
    "reopened head must include emitted token boundary: emitted={} reopened={}",
    emitted_token.log_index,
    status.head_log_index
  );
  let exported = reopened
    .primary_export_log_transport_json(None, 32, 1024 * 1024, false)
    .expect("export log after crash reopen");
  let exported_json: serde_json::Value = serde_json::from_str(&exported).expect("parse export");
  let exported_has_token = exported_json["frames"]
    .as_array()
    .expect("frames array")
    .iter()
    .any(|frame| {
      frame["epoch"].as_u64() == Some(emitted_token.epoch)
        && frame["log_index"].as_u64() == Some(emitted_token.log_index)
    });
  assert!(
    exported_has_token,
    "persisted log export must include emitted token {}:{}",
    emitted_token.epoch, emitted_token.log_index
  );

  close_single_file(reopened).expect("close reopened primary");
}
