use std::collections::HashSet;
use std::sync::{Arc, Barrier};

use kitedb::core::single_file::{close_single_file, open_single_file, SingleFileOpenOptions};
use kitedb::replication::primary::default_replication_sidecar_path;
use kitedb::replication::types::ReplicationRole;

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
