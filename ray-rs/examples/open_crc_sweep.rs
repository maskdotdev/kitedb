use kitedb::core::single_file::{
  close_single_file, open_single_file, SingleFileOpenOptions, SyncMode,
};
use kitedb::core::snapshot::reader::{ParseSnapshotOptions, SnapshotCrcProfile, SnapshotData};
use kitedb::types::{DbHeaderV1, SectionId, SnapshotFlags};
use kitedb::util::mmap::map_file;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const DEFAULT_NODE_COUNT: usize = 10_000;
const DEFAULT_VECTOR_DIM: usize = 128;
const OPEN_ROUNDS: usize = 20;
const PARSE_ROUNDS: usize = 40;

#[derive(Clone, Copy)]
struct ChunkConfig {
  label: &'static str,
  bytes: Option<usize>,
}

#[derive(Default)]
struct SampleSet {
  open_close_ns: Vec<u64>,
  parse_ns: Vec<u64>,
  crc_ns: Vec<u64>,
}

fn parse_bool_env(name: &str, default: bool) -> bool {
  std::env::var(name)
    .ok()
    .map(|value| {
      let value = value.to_ascii_lowercase();
      value == "1" || value == "true" || value == "yes"
    })
    .unwrap_or(default)
}

fn parse_usize_env(name: &str, default: usize) -> usize {
  std::env::var(name)
    .ok()
    .and_then(|value| value.parse::<usize>().ok())
    .unwrap_or(default)
}

fn fixture_open_options() -> SingleFileOpenOptions {
  SingleFileOpenOptions::new()
    .sync_mode(SyncMode::Normal)
    .wal_size(128 * 1024 * 1024)
    .auto_checkpoint(false)
    .background_checkpoint(false)
    .disable_checkpoint_compression()
}

fn build_vector_fixture(path: &Path, node_count: usize, vector_dim: usize, verbose: bool) {
  let db = open_single_file(path, fixture_open_options()).expect("open fixture db");

  db.begin(false).expect("begin");
  let prop_key_id = db.define_propkey("embedding").expect("define propkey");
  let vector: Vec<f32> = (0..vector_dim).map(|i| i as f32 + 1.0).collect();
  for i in 0..node_count {
    let node_id = db.create_node(Some(&format!("n{i}"))).expect("create node");
    db.set_node_vector(node_id, prop_key_id, &vector)
      .expect("set node vector");
  }
  db.commit().expect("commit");
  db.checkpoint().expect("checkpoint");
  close_single_file(db).expect("close fixture db");

  if verbose {
    eprintln!(
      "[open_crc_sweep] fixture ready nodes={node_count} dim={vector_dim} path={}",
      path.display(),
    );
  }
}

fn median(values: &[u64]) -> u64 {
  assert!(!values.is_empty());
  let mut sorted = values.to_vec();
  sorted.sort_unstable();
  sorted[sorted.len() / 2]
}

fn median_f64(values: &[f64]) -> f64 {
  assert!(!values.is_empty());
  let mut sorted = values.to_vec();
  sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
  sorted[sorted.len() / 2]
}

fn ns_to_ms(ns: u64) -> f64 {
  (ns as f64) / 1_000_000.0
}

fn baseline_delta_pct(value: u64, baseline: u64) -> f64 {
  if baseline == 0 {
    return 0.0;
  }
  ((value as f64 - baseline as f64) / baseline as f64) * 100.0
}

fn print_summary(
  chunks: &[ChunkConfig],
  samples: &[SampleSet],
  include_open: bool,
  include_parse: bool,
) {
  println!();
  println!(
    "{:<10} {:>12} {:>12} {:>12} {:>12} {:>12}",
    "chunk", "open_ms", "open_delta", "parse_ms", "crc_ms", "crc_ratio"
  );
  println!("{}", "-".repeat(74));

  let open_baseline = samples
    .first()
    .map(|s| median(&s.open_close_ns))
    .unwrap_or(0);
  let parse_baseline = samples.first().map(|s| median(&s.parse_ns)).unwrap_or(0);

  for (idx, chunk) in chunks.iter().enumerate() {
    let open_median = if include_open {
      median(&samples[idx].open_close_ns)
    } else {
      0
    };
    let parse_median = if include_parse {
      median(&samples[idx].parse_ns)
    } else {
      0
    };
    let crc_median = if include_parse {
      median(&samples[idx].crc_ns)
    } else {
      0
    };
    let crc_ratio = if parse_median > 0 {
      (crc_median as f64) / (parse_median as f64)
    } else {
      0.0
    };

    let open_delta = if include_open {
      baseline_delta_pct(open_median, open_baseline)
    } else {
      0.0
    };
    let parse_delta = if include_parse {
      baseline_delta_pct(parse_median, parse_baseline)
    } else {
      0.0
    };

    println!(
      "{:<10} {:>12.3} {:>11.2}% {:>12.3} {:>12.3} {:>11.2}x",
      chunk.label,
      ns_to_ms(open_median),
      open_delta,
      ns_to_ms(parse_median),
      ns_to_ms(crc_median),
      crc_ratio
    );

    if include_parse && parse_delta.abs() > 0.0 {
      let _ = parse_delta;
    }
  }
}

fn read_snapshot_offset(path: &Path) -> (usize, Arc<kitedb::util::mmap::Mmap>) {
  let file = File::open(path).expect("open fixture file");
  let mmap = Arc::new(map_file(&file).expect("mmap fixture file"));
  let header = DbHeaderV1::parse(&mmap[..4096]).expect("parse db header");
  let offset = (header.snapshot_start_page * header.page_size as u64) as usize;
  assert!(offset > 0, "fixture should contain snapshot pages");
  (offset, mmap)
}

fn sample_parse_crc(
  mmap: &Arc<kitedb::util::mmap::Mmap>,
  snapshot_offset: usize,
  chunk: Option<usize>,
) -> (u64, u64) {
  let sink = Arc::new(Mutex::new(None::<SnapshotCrcProfile>));
  let options = ParseSnapshotOptions {
    skip_crc_validation: false,
    crc_chunk_size: chunk,
    crc_profile_sink: Some(Arc::clone(&sink)),
  };
  let parse_start = Instant::now();
  let snapshot =
    SnapshotData::parse_at_offset(Arc::clone(mmap), snapshot_offset, &options).expect("parse");
  let parse_ns = parse_start.elapsed().as_nanos() as u64;
  std::hint::black_box(snapshot);

  let profile = sink
    .lock()
    .expect("lock crc profile")
    .clone()
    .expect("crc profile");
  (parse_ns, profile.total_ns)
}

fn sample_open_close(path: &Path, chunk: Option<usize>) -> u64 {
  match chunk {
    Some(bytes) => std::env::set_var("KITEDB_SNAPSHOT_CRC_CHUNK_BYTES", bytes.to_string()),
    None => std::env::remove_var("KITEDB_SNAPSHOT_CRC_CHUNK_BYTES"),
  }

  let start = Instant::now();
  let db = open_single_file(path, fixture_open_options()).expect("open");
  close_single_file(db).expect("close");
  start.elapsed().as_nanos() as u64
}

fn main() {
  let verbose = parse_bool_env("OPEN_CRC_SWEEP_VERBOSE", true);
  let open_rounds = parse_usize_env("OPEN_CRC_SWEEP_OPEN_ROUNDS", OPEN_ROUNDS);
  let parse_rounds = parse_usize_env("OPEN_CRC_SWEEP_PARSE_ROUNDS", PARSE_ROUNDS);
  let node_count = parse_usize_env("OPEN_CRC_SWEEP_NODES", DEFAULT_NODE_COUNT);
  let vector_dim = parse_usize_env("OPEN_CRC_SWEEP_DIM", DEFAULT_VECTOR_DIM);

  let temp_dir = tempfile::tempdir().expect("temp dir");
  let path: PathBuf = temp_dir.path().join("open-crc-sweep.kitedb");
  build_vector_fixture(&path, node_count, vector_dim, verbose);
  let (snapshot_offset, mmap) = read_snapshot_offset(&path);
  let baseline_snapshot = SnapshotData::parse_at_offset(
    Arc::clone(&mmap),
    snapshot_offset,
    &ParseSnapshotOptions::default(),
  )
  .expect("baseline snapshot parse");
  let vector_data_bytes = baseline_snapshot
    .section_data_shared(SectionId::VectorData)
    .map(|bytes| bytes.as_ref().len())
    .unwrap_or(0);
  let has_vectors = baseline_snapshot
    .header
    .flags
    .contains(SnapshotFlags::HAS_VECTORS);
  let num_nodes = baseline_snapshot.header.num_nodes;

  let chunks = vec![
    ChunkConfig {
      label: "default",
      bytes: None,
    },
    ChunkConfig {
      label: "256KB",
      bytes: Some(256 * 1024),
    },
    ChunkConfig {
      label: "1MB",
      bytes: Some(1024 * 1024),
    },
    ChunkConfig {
      label: "4MB",
      bytes: Some(4 * 1024 * 1024),
    },
  ];

  let mut samples: Vec<SampleSet> = (0..chunks.len()).map(|_| SampleSet::default()).collect();

  for _ in 0..parse_rounds {
    for (i, chunk) in chunks.iter().enumerate() {
      let (parse_ns, crc_ns) = sample_parse_crc(&mmap, snapshot_offset, chunk.bytes);
      samples[i].parse_ns.push(parse_ns);
      samples[i].crc_ns.push(crc_ns);
    }
  }

  for _ in 0..open_rounds {
    for (i, chunk) in chunks.iter().enumerate() {
      let open_ns = sample_open_close(&path, chunk.bytes);
      samples[i].open_close_ns.push(open_ns);
    }
  }

  std::env::remove_var("KITEDB_SNAPSHOT_CRC_CHUNK_BYTES");

  let crc_ratios: Vec<f64> = samples
    .iter()
    .map(|sample| {
      let parse_median = median(&sample.parse_ns) as f64;
      let crc_median = median(&sample.crc_ns) as f64;
      if parse_median > 0.0 {
        crc_median / parse_median
      } else {
        0.0
      }
    })
    .collect();

  if verbose {
    println!(
      "[open_crc_sweep] rounds parse={parse_rounds} open={open_rounds} fixture_nodes={node_count} fixture_dim={vector_dim} snapshot_nodes={num_nodes} has_vectors={has_vectors} vector_data_bytes={vector_data_bytes} snapshot_offset={snapshot_offset}"
    );
    println!(
      "[open_crc_sweep] median_crc_ratio={:.2}x",
      median_f64(&crc_ratios)
    );
  }

  print_summary(&chunks, &samples, true, true);
}
