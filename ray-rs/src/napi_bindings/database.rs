//! NAPI bindings for SingleFileDB
//!
//! Provides Node.js/Bun access to the single-file database format.

use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::Mutex;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::traversal::{
  JsPathConfig, JsPathResult, JsTraversalDirection, JsTraversalResult, JsTraversalStep,
  JsTraverseOptions,
};
use crate::api::pathfinding::{bfs, dijkstra, yen_k_shortest, PathConfig};
use crate::api::traversal::{
  TraversalBuilder as RustTraversalBuilder, TraversalDirection, TraverseOptions,
};
use crate::cache::manager::CacheManagerStats;
use crate::constants::{EXT_RAYDB, MANIFEST_FILENAME, SNAPSHOTS_DIR, WAL_DIR};
use crate::core::single_file::{
  close_single_file, is_single_file_path, open_single_file, SingleFileDB as RustSingleFileDB,
  SingleFileOpenOptions as RustOpenOptions, SyncMode as RustSyncMode,
};
use crate::export as ray_export;
use crate::graph::db::{
  close_graph_db, open_graph_db as open_multi_file, GraphDB as RustGraphDB,
  OpenOptions as GraphOpenOptions, TxState as GraphTxState,
};
use crate::graph::definitions::define_label as graph_define_label;
use crate::graph::edges::{
  add_edge as graph_add_edge, del_edge_prop as graph_del_edge_prop,
  delete_edge as graph_delete_edge, edge_exists_db, get_edge_prop_db, get_edge_props_db,
  set_edge_prop as graph_set_edge_prop,
};
use crate::graph::iterators::{
  count_edges as graph_count_edges, count_nodes as graph_count_nodes,
  list_edges as graph_list_edges, list_in_edges, list_nodes as graph_list_nodes, list_out_edges,
  ListEdgesOptions,
};
use crate::graph::key_index::get_node_key as graph_get_node_key;
use crate::graph::nodes::{
  add_node_label as graph_add_node_label, create_node as graph_create_node,
  del_node_prop as graph_del_node_prop, delete_node as graph_delete_node, get_node_by_key_db,
  get_node_labels_db, get_node_prop_db, get_node_props_db, node_exists_db, node_has_label_db,
  remove_node_label as graph_remove_node_label, set_node_prop as graph_set_node_prop, NodeOpts,
};
use crate::graph::tx::{
  begin_read_tx as graph_begin_read_tx, begin_tx as graph_begin_tx, commit as graph_commit,
  rollback as graph_rollback, TxHandle as GraphTxHandle,
};
use crate::graph::vectors::{
  delete_node_vector as graph_delete_node_vector, get_node_vector_db as graph_get_node_vector_db,
  has_node_vector_db as graph_has_node_vector_db, set_node_vector as graph_set_node_vector,
};
use crate::streaming;
use crate::types::{
  CheckResult as RustCheckResult, DeltaState, ETypeId, Edge, NodeId, PropKeyId, PropValue,
};
use serde_json;

// ============================================================================
// Sync Mode
// ============================================================================

/// Synchronization mode for WAL writes
///
/// Controls the durability vs performance trade-off for commits.
/// - Full: Fsync on every commit (durable to OS, slowest)
/// - Normal: Fsync only on checkpoint (~1000x faster, safe from app crash)
/// - Off: No fsync (fastest, data may be lost on any crash)
#[napi(string_enum)]
#[derive(Debug)]
pub enum JsSyncMode {
  /// Fsync on every commit (durable to OS, slowest)
  Full,
  /// Fsync on checkpoint only (balanced)
  Normal,
  /// No fsync (fastest, least safe)
  Off,
}

impl From<JsSyncMode> for RustSyncMode {
  fn from(mode: JsSyncMode) -> Self {
    match mode {
      JsSyncMode::Full => RustSyncMode::Full,
      JsSyncMode::Normal => RustSyncMode::Normal,
      JsSyncMode::Off => RustSyncMode::Off,
    }
  }
}

// ============================================================================
// Open Options
// ============================================================================

/// Options for opening a database
#[napi(object)]
#[derive(Debug, Default)]
pub struct OpenOptions {
  /// Open in read-only mode
  pub read_only: Option<bool>,
  /// Create database if it doesn't exist
  pub create_if_missing: Option<bool>,
  /// Acquire file lock (multi-file only)
  pub lock_file: Option<bool>,
  /// Require locking support (multi-file only)
  pub require_locking: Option<bool>,
  /// Enable MVCC (multi-file only)
  pub mvcc: Option<bool>,
  /// MVCC GC interval in ms (multi-file only)
  pub mvcc_gc_interval_ms: Option<i64>,
  /// MVCC retention in ms (multi-file only)
  pub mvcc_retention_ms: Option<i64>,
  /// MVCC max version chain depth (multi-file only)
  pub mvcc_max_chain_depth: Option<u32>,
  /// Page size in bytes (default 4096)
  pub page_size: Option<u32>,
  /// WAL size in bytes (default 1MB)
  pub wal_size: Option<u32>,
  /// Enable auto-checkpoint when WAL usage exceeds threshold
  pub auto_checkpoint: Option<bool>,
  /// WAL usage threshold (0.0-1.0) to trigger auto-checkpoint
  pub checkpoint_threshold: Option<f64>,
  /// Use background (non-blocking) checkpoint
  pub background_checkpoint: Option<bool>,
  /// Enable caching
  pub cache_enabled: Option<bool>,
  /// Max node properties in cache
  pub cache_max_node_props: Option<i64>,
  /// Max edge properties in cache
  pub cache_max_edge_props: Option<i64>,
  /// Max traversal cache entries
  pub cache_max_traversal_entries: Option<i64>,
  /// Max query cache entries
  pub cache_max_query_entries: Option<i64>,
  /// Query cache TTL in milliseconds
  pub cache_query_ttl_ms: Option<i64>,
  /// Sync mode: "Full", "Normal", or "Off" (default: "Full")
  pub sync_mode: Option<JsSyncMode>,
}

impl From<OpenOptions> for RustOpenOptions {
  fn from(opts: OpenOptions) -> Self {
    use crate::types::{CacheOptions, PropertyCacheConfig, QueryCacheConfig, TraversalCacheConfig};

    let mut rust_opts = RustOpenOptions::new();
    if let Some(v) = opts.read_only {
      rust_opts = rust_opts.read_only(v);
    }
    if let Some(v) = opts.create_if_missing {
      rust_opts = rust_opts.create_if_missing(v);
    }
    if let Some(v) = opts.page_size {
      rust_opts = rust_opts.page_size(v as usize);
    }
    if let Some(v) = opts.wal_size {
      rust_opts = rust_opts.wal_size(v as usize);
    }
    if let Some(v) = opts.auto_checkpoint {
      rust_opts = rust_opts.auto_checkpoint(v);
    }
    if let Some(v) = opts.checkpoint_threshold {
      rust_opts = rust_opts.checkpoint_threshold(v);
    }
    if let Some(v) = opts.background_checkpoint {
      rust_opts = rust_opts.background_checkpoint(v);
    }

    // Cache options
    if opts.cache_enabled == Some(true) {
      let property_cache = Some(PropertyCacheConfig {
        max_node_props: opts.cache_max_node_props.unwrap_or(10000) as usize,
        max_edge_props: opts.cache_max_edge_props.unwrap_or(10000) as usize,
      });

      let traversal_cache = Some(TraversalCacheConfig {
        max_entries: opts.cache_max_traversal_entries.unwrap_or(5000) as usize,
        max_neighbors_per_entry: 100,
      });

      let query_cache = Some(QueryCacheConfig {
        max_entries: opts.cache_max_query_entries.unwrap_or(1000) as usize,
        ttl_ms: opts.cache_query_ttl_ms.map(|v| v as u64),
      });

      rust_opts = rust_opts.cache(Some(CacheOptions {
        enabled: true,
        property_cache,
        traversal_cache,
        query_cache,
      }));
    }

    // Sync mode
    if let Some(mode) = opts.sync_mode {
      rust_opts = rust_opts.sync_mode(mode.into());
    }

    rust_opts
  }
}

impl OpenOptions {
  fn to_graph_options(&self) -> GraphOpenOptions {
    let mut opts = GraphOpenOptions::new();

    if let Some(v) = self.read_only {
      opts.read_only = v;
    }
    if let Some(v) = self.create_if_missing {
      opts.create_if_missing = v;
    }
    if let Some(v) = self.lock_file {
      opts.lock_file = v;
    }
    if let Some(v) = self.mvcc {
      opts.mvcc = v;
    }

    opts
  }
}

// ============================================================================
// Database Statistics
// ============================================================================

/// Database statistics
#[napi(object)]
pub struct DbStats {
  pub snapshot_gen: i64,
  pub snapshot_nodes: i64,
  pub snapshot_edges: i64,
  pub snapshot_max_node_id: i64,
  pub delta_nodes_created: i64,
  pub delta_nodes_deleted: i64,
  pub delta_edges_added: i64,
  pub delta_edges_deleted: i64,
  pub wal_bytes: i64,
  pub recommend_compact: bool,
}

/// Options for export
#[napi(object)]
pub struct ExportOptions {
  pub include_nodes: Option<bool>,
  pub include_edges: Option<bool>,
  pub include_schema: Option<bool>,
  pub pretty: Option<bool>,
}

impl ExportOptions {
  fn to_rust(self) -> ray_export::ExportOptions {
    let mut opts = ray_export::ExportOptions::default();
    if let Some(v) = self.include_nodes {
      opts.include_nodes = v;
    }
    if let Some(v) = self.include_edges {
      opts.include_edges = v;
    }
    if let Some(v) = self.include_schema {
      opts.include_schema = v;
    }
    if let Some(v) = self.pretty {
      opts.pretty = v;
    }
    opts
  }
}

/// Options for import
#[napi(object)]
pub struct ImportOptions {
  pub skip_existing: Option<bool>,
  pub batch_size: Option<i64>,
}

impl ImportOptions {
  fn to_rust(self) -> ray_export::ImportOptions {
    let mut opts = ray_export::ImportOptions::default();
    if let Some(v) = self.skip_existing {
      opts.skip_existing = v;
    }
    if let Some(v) = self.batch_size {
      if v > 0 {
        opts.batch_size = v as usize;
      }
    }
    opts
  }
}

/// Export result
#[napi(object)]
pub struct ExportResult {
  pub node_count: i64,
  pub edge_count: i64,
}

/// Import result
#[napi(object)]
pub struct ImportResult {
  pub node_count: i64,
  pub edge_count: i64,
  pub skipped: i64,
}

// =============================================================================
// Streaming / Pagination Options
// =============================================================================

/// Options for streaming node/edge batches
#[napi(object)]
#[derive(Debug, Default)]
pub struct StreamOptions {
  /// Number of items per batch (default: 1000)
  pub batch_size: Option<i64>,
}

impl StreamOptions {
  fn to_rust(self) -> Result<crate::streaming::StreamOptions> {
    let batch_size = self.batch_size.unwrap_or(0);
    if batch_size < 0 {
      return Err(Error::from_reason("batchSize must be non-negative"));
    }
    Ok(crate::streaming::StreamOptions {
      batch_size: batch_size as usize,
    })
  }
}

/// Options for cursor-based pagination
#[napi(object)]
#[derive(Debug, Default)]
pub struct PaginationOptions {
  /// Number of items per page (default: 100)
  pub limit: Option<i64>,
  /// Cursor from previous page
  pub cursor: Option<String>,
}

impl PaginationOptions {
  fn to_rust(self) -> Result<crate::streaming::PaginationOptions> {
    let limit = self.limit.unwrap_or(0);
    if limit < 0 {
      return Err(Error::from_reason("limit must be non-negative"));
    }
    Ok(crate::streaming::PaginationOptions {
      limit: limit as usize,
      cursor: self.cursor,
    })
  }
}

/// Node entry with properties
#[napi(object)]
pub struct NodeWithProps {
  pub id: i64,
  pub key: Option<String>,
  pub props: Vec<JsNodeProp>,
}

/// Edge entry with properties
#[napi(object)]
pub struct EdgeWithProps {
  pub src: i64,
  pub etype: u32,
  pub dst: i64,
  pub props: Vec<JsNodeProp>,
}

/// Page of node IDs
#[napi(object)]
pub struct NodePage {
  pub items: Vec<i64>,
  pub next_cursor: Option<String>,
  pub has_more: bool,
  pub total: Option<i64>,
}

/// Page of edges
#[napi(object)]
pub struct EdgePage {
  pub items: Vec<JsFullEdge>,
  pub next_cursor: Option<String>,
  pub has_more: bool,
  pub total: Option<i64>,
}

/// Database check result
#[napi(object)]
pub struct CheckResult {
  pub valid: bool,
  pub errors: Vec<String>,
  pub warnings: Vec<String>,
}

impl From<RustCheckResult> for CheckResult {
  fn from(result: RustCheckResult) -> Self {
    CheckResult {
      valid: result.valid,
      errors: result.errors,
      warnings: result.warnings,
    }
  }
}

/// Cache statistics
#[napi(object)]
pub struct JsCacheStats {
  pub property_cache_hits: i64,
  pub property_cache_misses: i64,
  pub property_cache_size: i64,
  pub traversal_cache_hits: i64,
  pub traversal_cache_misses: i64,
  pub traversal_cache_size: i64,
  pub query_cache_hits: i64,
  pub query_cache_misses: i64,
  pub query_cache_size: i64,
}

/// Cache layer metrics
#[napi(object)]
pub struct CacheLayerMetrics {
  pub hits: i64,
  pub misses: i64,
  pub hit_rate: f64,
  pub size: i64,
  pub max_size: i64,
  pub utilization_percent: f64,
}

/// Cache metrics
#[napi(object)]
pub struct CacheMetrics {
  pub enabled: bool,
  pub property_cache: CacheLayerMetrics,
  pub traversal_cache: CacheLayerMetrics,
  pub query_cache: CacheLayerMetrics,
}

/// Data metrics
#[napi(object)]
pub struct DataMetrics {
  pub node_count: i64,
  pub edge_count: i64,
  pub delta_nodes_created: i64,
  pub delta_nodes_deleted: i64,
  pub delta_edges_added: i64,
  pub delta_edges_deleted: i64,
  pub snapshot_generation: i64,
  pub max_node_id: i64,
  pub schema_labels: i64,
  pub schema_etypes: i64,
  pub schema_prop_keys: i64,
}

/// MVCC metrics
#[napi(object)]
pub struct MvccMetrics {
  pub enabled: bool,
  pub active_transactions: i64,
  pub versions_pruned: i64,
  pub gc_runs: i64,
  pub min_active_timestamp: i64,
}

/// Memory metrics
#[napi(object)]
pub struct MemoryMetrics {
  pub delta_estimate_bytes: i64,
  pub cache_estimate_bytes: i64,
  pub snapshot_bytes: i64,
  pub total_estimate_bytes: i64,
}

/// Database metrics
#[napi(object)]
pub struct DatabaseMetrics {
  pub path: String,
  pub is_single_file: bool,
  pub read_only: bool,
  pub data: DataMetrics,
  pub cache: CacheMetrics,
  pub mvcc: Option<MvccMetrics>,
  pub memory: MemoryMetrics,
  /// Timestamp in milliseconds since epoch
  pub collected_at: i64,
}

/// Health check entry
#[napi(object)]
pub struct HealthCheckEntry {
  pub name: String,
  pub passed: bool,
  pub message: String,
}

/// Health check result
#[napi(object)]
pub struct HealthCheckResult {
  pub healthy: bool,
  pub checks: Vec<HealthCheckEntry>,
}

// ============================================================================
// Property Value (JS-compatible)
// ============================================================================

/// Property value types
#[napi(string_enum)]
#[derive(Clone)]
pub enum PropType {
  Null,
  Bool,
  Int,
  Float,
  String,
  Vector,
}

/// Property value wrapper for JS
#[napi(object)]
#[derive(Clone)]
pub struct JsPropValue {
  pub prop_type: PropType,
  pub bool_value: Option<bool>,
  pub int_value: Option<i64>,
  pub float_value: Option<f64>,
  pub string_value: Option<String>,
  pub vector_value: Option<Vec<f64>>,
}

impl From<PropValue> for JsPropValue {
  fn from(value: PropValue) -> Self {
    match value {
      PropValue::Null => JsPropValue {
        prop_type: PropType::Null,
        bool_value: None,
        int_value: None,
        float_value: None,
        string_value: None,
        vector_value: None,
      },
      PropValue::Bool(v) => JsPropValue {
        prop_type: PropType::Bool,
        bool_value: Some(v),
        int_value: None,
        float_value: None,
        string_value: None,
        vector_value: None,
      },
      PropValue::I64(v) => JsPropValue {
        prop_type: PropType::Int,
        bool_value: None,
        int_value: Some(v),
        float_value: None,
        string_value: None,
        vector_value: None,
      },
      PropValue::F64(v) => JsPropValue {
        prop_type: PropType::Float,
        bool_value: None,
        int_value: None,
        float_value: Some(v),
        string_value: None,
        vector_value: None,
      },
      PropValue::String(v) => JsPropValue {
        prop_type: PropType::String,
        bool_value: None,
        int_value: None,
        float_value: None,
        string_value: Some(v),
        vector_value: None,
      },
      PropValue::VectorF32(v) => JsPropValue {
        prop_type: PropType::Vector,
        bool_value: None,
        int_value: None,
        float_value: None,
        string_value: None,
        vector_value: Some(v.iter().map(|&x| x as f64).collect()),
      },
    }
  }
}

impl From<JsPropValue> for PropValue {
  fn from(value: JsPropValue) -> Self {
    match value.prop_type {
      PropType::Null => PropValue::Null,
      PropType::Bool => PropValue::Bool(value.bool_value.unwrap_or(false)),
      PropType::Int => PropValue::I64(value.int_value.unwrap_or(0)),
      PropType::Float => PropValue::F64(value.float_value.unwrap_or(0.0)),
      PropType::String => PropValue::String(value.string_value.unwrap_or_default()),
      PropType::Vector => {
        let vector = value.vector_value.unwrap_or_default();
        PropValue::VectorF32(vector.iter().map(|&x| x as f32).collect())
      }
    }
  }
}

// ============================================================================
// Edge Result
// ============================================================================

/// Edge representation for JS (neighbor style)
#[napi(object)]
pub struct JsEdge {
  pub etype: u32,
  pub node_id: i64,
}

/// Full edge representation for JS (src, etype, dst)
#[napi(object)]
pub struct JsFullEdge {
  pub src: i64,
  pub etype: u32,
  pub dst: i64,
}

// ============================================================================
// Node Property Result
// ============================================================================

/// Node property key-value pair for JS
#[napi(object)]
pub struct JsNodeProp {
  pub key_id: u32,
  pub value: JsPropValue,
}

// ============================================================================
// Database NAPI Wrapper (single-file + multi-file)
// ============================================================================

enum DatabaseInner {
  SingleFile(RustSingleFileDB),
  Graph(RustGraphDB),
}

/// Graph database handle (single-file or multi-file)
#[napi]
pub struct Database {
  inner: Option<DatabaseInner>,
  graph_tx: Mutex<Option<GraphTxState>>, // Only used for multi-file GraphDB
}

#[napi]
impl Database {
  /// Open a database file
  #[napi(factory)]
  pub fn open(path: String, options: Option<OpenOptions>) -> Result<Database> {
    let options = options.unwrap_or_default();
    let path_buf = PathBuf::from(&path);

    if path_buf.exists() {
      if path_buf.is_dir() {
        let graph_opts = options.to_graph_options();
        let db = open_multi_file(&path_buf, graph_opts)
          .map_err(|e| Error::from_reason(format!("Failed to open database: {e}")))?;
        return Ok(Database {
          inner: Some(DatabaseInner::Graph(db)),
          graph_tx: Mutex::new(None),
        });
      }
    }

    let mut db_path = path_buf;
    if !is_single_file_path(&db_path) {
      db_path = PathBuf::from(format!("{path}.raydb"));
    }

    let opts: RustOpenOptions = options.into();
    let db = open_single_file(&db_path, opts)
      .map_err(|e| Error::from_reason(format!("Failed to open database: {e}")))?;
    Ok(Database {
      inner: Some(DatabaseInner::SingleFile(db)),
      graph_tx: Mutex::new(None),
    })
  }

  /// Close the database
  #[napi]
  pub fn close(&mut self) -> Result<()> {
    if let Some(db) = self.inner.take() {
      match db {
        DatabaseInner::SingleFile(db) => {
          close_single_file(db)
            .map_err(|e| Error::from_reason(format!("Failed to close database: {e}")))?;
        }
        DatabaseInner::Graph(db) => {
          close_graph_db(db)
            .map_err(|e| Error::from_reason(format!("Failed to close database: {e}")))?;
        }
      }
    }
    self.graph_tx.lock().take();
    Ok(())
  }

  /// Check if database is open
  #[napi(getter)]
  pub fn is_open(&self) -> bool {
    self.inner.is_some()
  }

  /// Get database path
  #[napi(getter)]
  pub fn path(&self) -> Result<String> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.path.to_string_lossy().to_string()),
      Some(DatabaseInner::Graph(db)) => Ok(db.path.to_string_lossy().to_string()),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Check if database is read-only
  #[napi(getter)]
  pub fn read_only(&self) -> Result<bool> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.read_only),
      Some(DatabaseInner::Graph(db)) => Ok(db.read_only),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Transaction Methods
  // ========================================================================

  /// Begin a transaction
  #[napi]
  pub fn begin(&self, read_only: Option<bool>) -> Result<i64> {
    let read_only = read_only.unwrap_or(false);
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let txid = db
          .begin(read_only)
          .map_err(|e| Error::from_reason(format!("Failed to begin transaction: {e}")))?;
        Ok(txid as i64)
      }
      Some(DatabaseInner::Graph(db)) => {
        let mut guard = self.graph_tx.lock();
        if guard.is_some() {
          return Err(Error::from_reason("Transaction already active"));
        }

        let handle = if read_only {
          graph_begin_read_tx(db)
            .map_err(|e| Error::from_reason(format!("Failed to begin transaction: {e}")))?
        } else {
          graph_begin_tx(db)
            .map_err(|e| Error::from_reason(format!("Failed to begin transaction: {e}")))?
        };
        let txid = handle.tx.txid as i64;
        *guard = Some(handle.tx);
        Ok(txid)
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Commit the current transaction
  #[napi]
  pub fn commit(&self) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .commit()
        .map_err(|e| Error::from_reason(format!("Failed to commit: {e}"))),
      Some(DatabaseInner::Graph(db)) => {
        let mut guard = self.graph_tx.lock();
        let tx_state = guard
          .take()
          .ok_or_else(|| Error::from_reason("No active transaction"))?;
        let mut handle = GraphTxHandle::new(db, tx_state);
        graph_commit(&mut handle)
          .map_err(|e| Error::from_reason(format!("Failed to commit: {e}")))?;
        Ok(())
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Rollback the current transaction
  #[napi]
  pub fn rollback(&self) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .rollback()
        .map_err(|e| Error::from_reason(format!("Failed to rollback: {e}"))),
      Some(DatabaseInner::Graph(db)) => {
        let mut guard = self.graph_tx.lock();
        let tx_state = guard
          .take()
          .ok_or_else(|| Error::from_reason("No active transaction"))?;
        let mut handle = GraphTxHandle::new(db, tx_state);
        graph_rollback(&mut handle)
          .map_err(|e| Error::from_reason(format!("Failed to rollback: {e}")))?;
        Ok(())
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Check if there's an active transaction
  #[napi]
  pub fn has_transaction(&self) -> Result<bool> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.has_transaction()),
      Some(DatabaseInner::Graph(_)) => Ok(self.graph_tx.lock().is_some()),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Node Operations
  // ========================================================================

  /// Create a new node
  #[napi]
  pub fn create_node(&self, key: Option<String>) -> Result<i64> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let node_id = db
          .create_node(key.as_deref())
          .map_err(|e| Error::from_reason(format!("Failed to create node: {e}")))?;
        Ok(node_id as i64)
      }
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        let mut opts = NodeOpts::new();
        if let Some(key) = key {
          opts = opts.with_key(key);
        }
        let node_id = graph_create_node(handle, opts)
          .map_err(|e| Error::from_reason(format!("Failed to create node: {e}")))?;
        Ok(node_id as i64)
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Delete a node
  #[napi]
  pub fn delete_node(&self, node_id: i64) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .delete_node(node_id as NodeId)
        .map_err(|e| Error::from_reason(format!("Failed to delete node: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        graph_delete_node(handle, node_id as NodeId)
          .map_err(|e| Error::from_reason(format!("Failed to delete node: {e}")))?;
        Ok(())
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Check if a node exists
  #[napi]
  pub fn node_exists(&self, node_id: i64) -> Result<bool> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.node_exists(node_id as NodeId)),
      Some(DatabaseInner::Graph(db)) => Ok(node_exists_db(db, node_id as NodeId)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get node by key
  #[napi]
  pub fn get_node_by_key(&self, key: String) -> Result<Option<i64>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_node_by_key(&key).map(|id| id as i64)),
      Some(DatabaseInner::Graph(db)) => Ok(get_node_by_key_db(db, &key).map(|id| id as i64)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get the key for a node
  #[napi]
  pub fn get_node_key(&self, node_id: i64) -> Result<Option<String>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_node_key(node_id as NodeId)),
      Some(DatabaseInner::Graph(db)) => {
        let delta = db.delta.read();
        Ok(graph_get_node_key(
          db.snapshot.as_ref(),
          &delta,
          node_id as NodeId,
        ))
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// List all node IDs
  #[napi]
  pub fn list_nodes(&self) -> Result<Vec<i64>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        Ok(db.list_nodes().into_iter().map(|id| id as i64).collect())
      }
      Some(DatabaseInner::Graph(db)) => Ok(
        graph_list_nodes(db)
          .into_iter()
          .map(|id| id as i64)
          .collect(),
      ),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Count all nodes
  #[napi]
  pub fn count_nodes(&self) -> Result<i64> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.count_nodes() as i64),
      Some(DatabaseInner::Graph(db)) => Ok(graph_count_nodes(db) as i64),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Edge Operations
  // ========================================================================

  /// Add an edge
  #[napi]
  pub fn add_edge(&self, src: i64, etype: u32, dst: i64) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .add_edge(src as NodeId, etype as ETypeId, dst as NodeId)
        .map_err(|e| Error::from_reason(format!("Failed to add edge: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        graph_add_edge(handle, src as NodeId, etype as ETypeId, dst as NodeId)
          .map_err(|e| Error::from_reason(format!("Failed to add edge: {e}")))?;
        Ok(())
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Add an edge by type name
  #[napi]
  pub fn add_edge_by_name(&self, src: i64, etype_name: String, dst: i64) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .add_edge_by_name(src as NodeId, &etype_name, dst as NodeId)
        .map_err(|e| Error::from_reason(format!("Failed to add edge: {e}"))),
      Some(DatabaseInner::Graph(db)) => {
        let etype = db
          .get_etype_id(&etype_name)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {etype_name}")))?;
        self.with_graph_tx(|handle| {
          graph_add_edge(handle, src as NodeId, etype, dst as NodeId)
            .map_err(|e| Error::from_reason(format!("Failed to add edge: {e}")))?;
          Ok(())
        })
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Delete an edge
  #[napi]
  pub fn delete_edge(&self, src: i64, etype: u32, dst: i64) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .delete_edge(src as NodeId, etype as ETypeId, dst as NodeId)
        .map_err(|e| Error::from_reason(format!("Failed to delete edge: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        graph_delete_edge(handle, src as NodeId, etype as ETypeId, dst as NodeId)
          .map_err(|e| Error::from_reason(format!("Failed to delete edge: {e}")))?;
        Ok(())
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Check if an edge exists
  #[napi]
  pub fn edge_exists(&self, src: i64, etype: u32, dst: i64) -> Result<bool> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        Ok(db.edge_exists(src as NodeId, etype as ETypeId, dst as NodeId))
      }
      Some(DatabaseInner::Graph(db)) => Ok(edge_exists_db(
        db,
        src as NodeId,
        etype as ETypeId,
        dst as NodeId,
      )),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get outgoing edges for a node
  #[napi]
  pub fn get_out_edges(&self, node_id: i64) -> Result<Vec<JsEdge>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_out_edges(node_id as NodeId)
          .into_iter()
          .map(|(etype, dst)| JsEdge {
            etype,
            node_id: dst as i64,
          })
          .collect(),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        list_out_edges(db, node_id as NodeId)
          .into_iter()
          .map(|edge| JsEdge {
            etype: edge.etype,
            node_id: edge.dst as i64,
          })
          .collect(),
      ),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get incoming edges for a node
  #[napi]
  pub fn get_in_edges(&self, node_id: i64) -> Result<Vec<JsEdge>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_in_edges(node_id as NodeId)
          .into_iter()
          .map(|(etype, src)| JsEdge {
            etype,
            node_id: src as i64,
          })
          .collect(),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        list_in_edges(db, node_id as NodeId)
          .into_iter()
          .map(|edge| JsEdge {
            etype: edge.etype,
            node_id: edge.dst as i64,
          })
          .collect(),
      ),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get out-degree for a node
  #[napi]
  pub fn get_out_degree(&self, node_id: i64) -> Result<i64> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_out_degree(node_id as NodeId) as i64),
      Some(DatabaseInner::Graph(db)) => Ok(list_out_edges(db, node_id as NodeId).len() as i64),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get in-degree for a node
  #[napi]
  pub fn get_in_degree(&self, node_id: i64) -> Result<i64> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_in_degree(node_id as NodeId) as i64),
      Some(DatabaseInner::Graph(db)) => Ok(list_in_edges(db, node_id as NodeId).len() as i64),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Count all edges
  #[napi]
  pub fn count_edges(&self) -> Result<i64> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.count_edges() as i64),
      Some(DatabaseInner::Graph(db)) => Ok(graph_count_edges(db, None) as i64),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// List all edges in the database
  ///
  /// Returns an array of {src, etype, dst} objects representing all edges.
  /// Optionally filter by edge type.
  #[napi]
  pub fn list_edges(&self, etype: Option<u32>) -> Result<Vec<JsFullEdge>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.list_edges(etype)
          .into_iter()
          .map(|e| JsFullEdge {
            src: e.src as i64,
            etype: e.etype,
            dst: e.dst as i64,
          })
          .collect(),
      ),
      Some(DatabaseInner::Graph(db)) => {
        let options = ListEdgesOptions { etype };
        Ok(
          graph_list_edges(db, options)
            .into_iter()
            .map(|e| JsFullEdge {
              src: e.src as i64,
              etype: e.etype,
              dst: e.dst as i64,
            })
            .collect(),
        )
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// List edges by type name
  ///
  /// Returns an array of {src, etype, dst} objects for the given edge type.
  #[napi]
  pub fn list_edges_by_name(&self, etype_name: String) -> Result<Vec<JsFullEdge>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let etype = db
          .get_etype_id(&etype_name)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {etype_name}")))?;
        Ok(
          db.list_edges(Some(etype))
            .into_iter()
            .map(|e| JsFullEdge {
              src: e.src as i64,
              etype: e.etype,
              dst: e.dst as i64,
            })
            .collect(),
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let etype = db
          .get_etype_id(&etype_name)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {etype_name}")))?;
        let options = ListEdgesOptions { etype: Some(etype) };
        Ok(
          graph_list_edges(db, options)
            .into_iter()
            .map(|e| JsFullEdge {
              src: e.src as i64,
              etype: e.etype,
              dst: e.dst as i64,
            })
            .collect(),
        )
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Count edges by type
  #[napi]
  pub fn count_edges_by_type(&self, etype: u32) -> Result<i64> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.count_edges_by_type(etype) as i64),
      Some(DatabaseInner::Graph(db)) => Ok(graph_count_edges(db, Some(etype)) as i64),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Count edges by type name
  #[napi]
  pub fn count_edges_by_name(&self, etype_name: String) -> Result<i64> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let etype = db
          .get_etype_id(&etype_name)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {etype_name}")))?;
        Ok(db.count_edges_by_type(etype) as i64)
      }
      Some(DatabaseInner::Graph(db)) => {
        let etype = db
          .get_etype_id(&etype_name)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {etype_name}")))?;
        Ok(graph_count_edges(db, Some(etype)) as i64)
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Streaming and Pagination
  // ========================================================================

  /// Stream nodes in batches
  #[napi]
  pub fn stream_nodes(&self, options: Option<StreamOptions>) -> Result<Vec<Vec<i64>>> {
    let options = options.unwrap_or_default().to_rust()?;
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        streaming::stream_nodes_single(db, options)
          .into_iter()
          .map(|batch| batch.into_iter().map(|id| id as i64).collect())
          .collect(),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        streaming::stream_nodes_graph(db, options)
          .into_iter()
          .map(|batch| batch.into_iter().map(|id| id as i64).collect())
          .collect(),
      ),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Stream nodes with properties in batches
  #[napi]
  pub fn stream_nodes_with_props(
    &self,
    options: Option<StreamOptions>,
  ) -> Result<Vec<Vec<NodeWithProps>>> {
    let options = options.unwrap_or_default().to_rust()?;
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let batches = streaming::stream_nodes_single(db, options);
        Ok(
          batches
            .into_iter()
            .map(|batch| {
              batch
                .into_iter()
                .map(|node_id| {
                  let key = db.get_node_key(node_id as NodeId);
                  let props = db.get_node_props(node_id as NodeId).unwrap_or_default();
                  let props = props
                    .into_iter()
                    .map(|(k, v)| JsNodeProp {
                      key_id: k,
                      value: v.into(),
                    })
                    .collect();
                  NodeWithProps {
                    id: node_id as i64,
                    key,
                    props,
                  }
                })
                .collect()
            })
            .collect(),
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let batches = streaming::stream_nodes_graph(db, options);
        Ok(
          batches
            .into_iter()
            .map(|batch| {
              batch
                .into_iter()
                .map(|node_id| {
                  let key = {
                    let delta = db.delta.read();
                    graph_get_node_key(db.snapshot.as_ref(), &delta, node_id)
                  };
                  let props = get_node_props_db(db, node_id).unwrap_or_default();
                  let props = props
                    .into_iter()
                    .map(|(k, v)| JsNodeProp {
                      key_id: k,
                      value: v.into(),
                    })
                    .collect();
                  NodeWithProps {
                    id: node_id as i64,
                    key,
                    props,
                  }
                })
                .collect()
            })
            .collect(),
        )
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Stream edges in batches
  #[napi]
  pub fn stream_edges(&self, options: Option<StreamOptions>) -> Result<Vec<Vec<JsFullEdge>>> {
    let options = options.unwrap_or_default().to_rust()?;
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        streaming::stream_edges_single(db, options)
          .into_iter()
          .map(|batch| {
            batch
              .into_iter()
              .map(|edge| JsFullEdge {
                src: edge.src as i64,
                etype: edge.etype,
                dst: edge.dst as i64,
              })
              .collect()
          })
          .collect(),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        streaming::stream_edges_graph(db, options)
          .into_iter()
          .map(|batch| {
            batch
              .into_iter()
              .map(|edge| JsFullEdge {
                src: edge.src as i64,
                etype: edge.etype,
                dst: edge.dst as i64,
              })
              .collect()
          })
          .collect(),
      ),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Stream edges with properties in batches
  #[napi]
  pub fn stream_edges_with_props(
    &self,
    options: Option<StreamOptions>,
  ) -> Result<Vec<Vec<EdgeWithProps>>> {
    let options = options.unwrap_or_default().to_rust()?;
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let batches = streaming::stream_edges_single(db, options);
        Ok(
          batches
            .into_iter()
            .map(|batch| {
              batch
                .into_iter()
                .map(|edge| {
                  let props = db
                    .get_edge_props(edge.src, edge.etype, edge.dst)
                    .unwrap_or_default();
                  let props = props
                    .into_iter()
                    .map(|(k, v)| JsNodeProp {
                      key_id: k,
                      value: v.into(),
                    })
                    .collect();
                  EdgeWithProps {
                    src: edge.src as i64,
                    etype: edge.etype,
                    dst: edge.dst as i64,
                    props,
                  }
                })
                .collect()
            })
            .collect(),
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let batches = streaming::stream_edges_graph(db, options);
        Ok(
          batches
            .into_iter()
            .map(|batch| {
              batch
                .into_iter()
                .map(|edge| {
                  let props =
                    get_edge_props_db(db, edge.src, edge.etype, edge.dst).unwrap_or_default();
                  let props = props
                    .into_iter()
                    .map(|(k, v)| JsNodeProp {
                      key_id: k,
                      value: v.into(),
                    })
                    .collect();
                  EdgeWithProps {
                    src: edge.src as i64,
                    etype: edge.etype,
                    dst: edge.dst as i64,
                    props,
                  }
                })
                .collect()
            })
            .collect(),
        )
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get a page of node IDs
  #[napi]
  pub fn get_nodes_page(&self, options: Option<PaginationOptions>) -> Result<NodePage> {
    let options = options.unwrap_or_default().to_rust()?;
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let page = streaming::get_nodes_page_single(db, options);
        Ok(NodePage {
          items: page.items.into_iter().map(|id| id as i64).collect(),
          next_cursor: page.next_cursor,
          has_more: page.has_more,
          total: Some(db.count_nodes() as i64),
        })
      }
      Some(DatabaseInner::Graph(db)) => {
        let page = streaming::get_nodes_page_graph(db, options);
        Ok(NodePage {
          items: page.items.into_iter().map(|id| id as i64).collect(),
          next_cursor: page.next_cursor,
          has_more: page.has_more,
          total: Some(graph_count_nodes(db) as i64),
        })
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get a page of edges
  #[napi]
  pub fn get_edges_page(&self, options: Option<PaginationOptions>) -> Result<EdgePage> {
    let options = options.unwrap_or_default().to_rust()?;
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let page = streaming::get_edges_page_single(db, options);
        Ok(EdgePage {
          items: page
            .items
            .into_iter()
            .map(|edge| JsFullEdge {
              src: edge.src as i64,
              etype: edge.etype,
              dst: edge.dst as i64,
            })
            .collect(),
          next_cursor: page.next_cursor,
          has_more: page.has_more,
          total: Some(db.count_edges() as i64),
        })
      }
      Some(DatabaseInner::Graph(db)) => {
        let page = streaming::get_edges_page_graph(db, options);
        Ok(EdgePage {
          items: page
            .items
            .into_iter()
            .map(|edge| JsFullEdge {
              src: edge.src as i64,
              etype: edge.etype,
              dst: edge.dst as i64,
            })
            .collect(),
          next_cursor: page.next_cursor,
          has_more: page.has_more,
          total: Some(graph_count_edges(db, None) as i64),
        })
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Property Operations
  // ========================================================================

  /// Set a node property
  #[napi]
  pub fn set_node_prop(&self, node_id: i64, key_id: u32, value: JsPropValue) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_node_prop(node_id as NodeId, key_id as PropKeyId, value.into())
        .map_err(|e| Error::from_reason(format!("Failed to set property: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        graph_set_node_prop(handle, node_id as NodeId, key_id as PropKeyId, value.into())
          .map_err(|e| Error::from_reason(format!("Failed to set property: {e}")))?;
        Ok(())
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Set a node property by key name
  #[napi]
  pub fn set_node_prop_by_name(
    &self,
    node_id: i64,
    key_name: String,
    value: JsPropValue,
  ) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_node_prop_by_name(node_id as NodeId, &key_name, value.into())
        .map_err(|e| Error::from_reason(format!("Failed to set property: {e}"))),
      Some(DatabaseInner::Graph(db)) => {
        let key_id = db
          .get_propkey_id(&key_name)
          .ok_or_else(|| Error::from_reason(format!("Unknown property key: {key_name}")))?;
        self.with_graph_tx(|handle| {
          graph_set_node_prop(handle, node_id as NodeId, key_id, value.into())
            .map_err(|e| Error::from_reason(format!("Failed to set property: {e}")))?;
          Ok(())
        })
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Delete a node property
  #[napi]
  pub fn delete_node_prop(&self, node_id: i64, key_id: u32) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .delete_node_prop(node_id as NodeId, key_id as PropKeyId)
        .map_err(|e| Error::from_reason(format!("Failed to delete property: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        graph_del_node_prop(handle, node_id as NodeId, key_id as PropKeyId)
          .map_err(|e| Error::from_reason(format!("Failed to delete property: {e}")))?;
        Ok(())
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get a specific node property
  #[napi]
  pub fn get_node_prop(&self, node_id: i64, key_id: u32) -> Result<Option<JsPropValue>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_node_prop(node_id as NodeId, key_id as PropKeyId)
          .map(|v| v.into()),
      ),
      Some(DatabaseInner::Graph(db)) => {
        Ok(get_node_prop_db(db, node_id as NodeId, key_id as PropKeyId).map(|v| v.into()))
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get all properties for a node (returns array of {key_id, value} pairs)
  #[napi]
  pub fn get_node_props(&self, node_id: i64) -> Result<Option<Vec<JsNodeProp>>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        Ok(db.get_node_props(node_id as NodeId).map(|props| {
          props
            .into_iter()
            .map(|(k, v)| JsNodeProp {
              key_id: k,
              value: v.into(),
            })
            .collect()
        }))
      }
      Some(DatabaseInner::Graph(db)) => Ok(get_node_props_db(db, node_id as NodeId).map(|props| {
        props
          .into_iter()
          .map(|(k, v)| JsNodeProp {
            key_id: k,
            value: v.into(),
          })
          .collect()
      })),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Edge Property Operations
  // ========================================================================

  /// Set an edge property
  #[napi]
  pub fn set_edge_prop(
    &self,
    src: i64,
    etype: u32,
    dst: i64,
    key_id: u32,
    value: JsPropValue,
  ) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_edge_prop(
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
          value.into(),
        )
        .map_err(|e| Error::from_reason(format!("Failed to set edge property: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        graph_set_edge_prop(
          handle,
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
          value.into(),
        )
        .map_err(|e| Error::from_reason(format!("Failed to set edge property: {e}")))?;
        Ok(())
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Set an edge property by key name
  #[napi]
  pub fn set_edge_prop_by_name(
    &self,
    src: i64,
    etype: u32,
    dst: i64,
    key_name: String,
    value: JsPropValue,
  ) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_edge_prop_by_name(
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          &key_name,
          value.into(),
        )
        .map_err(|e| Error::from_reason(format!("Failed to set edge property: {e}"))),
      Some(DatabaseInner::Graph(db)) => {
        let key_id = db
          .get_propkey_id(&key_name)
          .ok_or_else(|| Error::from_reason(format!("Unknown property key: {key_name}")))?;
        self.with_graph_tx(|handle| {
          graph_set_edge_prop(
            handle,
            src as NodeId,
            etype as ETypeId,
            dst as NodeId,
            key_id,
            value.into(),
          )
          .map_err(|e| Error::from_reason(format!("Failed to set edge property: {e}")))?;
          Ok(())
        })
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Delete an edge property
  #[napi]
  pub fn delete_edge_prop(&self, src: i64, etype: u32, dst: i64, key_id: u32) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .delete_edge_prop(
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
        )
        .map_err(|e| Error::from_reason(format!("Failed to delete edge property: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        graph_del_edge_prop(
          handle,
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
        )
        .map_err(|e| Error::from_reason(format!("Failed to delete edge property: {e}")))?;
        Ok(())
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get a specific edge property
  #[napi]
  pub fn get_edge_prop(
    &self,
    src: i64,
    etype: u32,
    dst: i64,
    key_id: u32,
  ) -> Result<Option<JsPropValue>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_edge_prop(
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
        )
        .map(|v| v.into()),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        get_edge_prop_db(
          db,
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
        )
        .map(|v| v.into()),
      ),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get all properties for an edge (returns array of {key_id, value} pairs)
  #[napi]
  pub fn get_edge_props(&self, src: i64, etype: u32, dst: i64) -> Result<Option<Vec<JsNodeProp>>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_edge_props(src as NodeId, etype as ETypeId, dst as NodeId)
          .map(|props| {
            props
              .into_iter()
              .map(|(k, v)| JsNodeProp {
                key_id: k,
                value: v.into(),
              })
              .collect()
          }),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        get_edge_props_db(db, src as NodeId, etype as ETypeId, dst as NodeId).map(|props| {
          props
            .into_iter()
            .map(|(k, v)| JsNodeProp {
              key_id: k,
              value: v.into(),
            })
            .collect()
        }),
      ),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Vector Operations
  // ========================================================================

  /// Set a vector embedding for a node
  #[napi]
  pub fn set_node_vector(&self, node_id: i64, prop_key_id: u32, vector: Vec<f64>) -> Result<()> {
    let vector_f32: Vec<f32> = vector.iter().map(|&v| v as f32).collect();
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_node_vector(node_id as NodeId, prop_key_id as PropKeyId, &vector_f32)
        .map_err(|e| Error::from_reason(format!("Failed to set vector: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        graph_set_node_vector(
          handle,
          node_id as NodeId,
          prop_key_id as PropKeyId,
          &vector_f32,
        )
        .map_err(|e| Error::from_reason(format!("Failed to set vector: {e}")))?;
        Ok(())
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get a vector embedding for a node
  #[napi]
  pub fn get_node_vector(&self, node_id: i64, prop_key_id: u32) -> Result<Option<Vec<f64>>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_node_vector(node_id as NodeId, prop_key_id as PropKeyId)
          .map(|v| v.iter().map(|&f| f as f64).collect()),
      ),
      Some(DatabaseInner::Graph(db)) => {
        let pending = {
          let guard = self.graph_tx.lock();
          guard
            .as_ref()
            .and_then(|tx| {
              tx.pending_vectors
                .get(&(node_id as NodeId, prop_key_id as PropKeyId))
            })
            .cloned()
        };

        if let Some(pending_vec) = pending {
          return Ok(pending_vec.map(|v| v.iter().map(|&f| f as f64).collect()));
        }

        Ok(
          graph_get_node_vector_db(db, node_id as NodeId, prop_key_id as PropKeyId)
            .map(|v| v.iter().map(|&f| f as f64).collect()),
        )
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Delete a vector embedding for a node
  #[napi]
  pub fn delete_node_vector(&self, node_id: i64, prop_key_id: u32) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .delete_node_vector(node_id as NodeId, prop_key_id as PropKeyId)
        .map_err(|e| Error::from_reason(format!("Failed to delete vector: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        graph_delete_node_vector(handle, node_id as NodeId, prop_key_id as PropKeyId)
          .map_err(|e| Error::from_reason(format!("Failed to delete vector: {e}")))?;
        Ok(())
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Check if a node has a vector embedding
  #[napi]
  pub fn has_node_vector(&self, node_id: i64, prop_key_id: u32) -> Result<bool> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        Ok(db.has_node_vector(node_id as NodeId, prop_key_id as PropKeyId))
      }
      Some(DatabaseInner::Graph(db)) => {
        let pending = {
          let guard = self.graph_tx.lock();
          guard
            .as_ref()
            .and_then(|tx| {
              tx.pending_vectors
                .get(&(node_id as NodeId, prop_key_id as PropKeyId))
            })
            .cloned()
        };

        if let Some(pending_vec) = pending {
          return Ok(pending_vec.is_some());
        }

        Ok(graph_has_node_vector_db(
          db,
          node_id as NodeId,
          prop_key_id as PropKeyId,
        ))
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Schema Operations
  // ========================================================================

  /// Get or create a label ID
  #[napi]
  pub fn get_or_create_label(&self, name: String) -> Result<u32> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_or_create_label(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_or_create_label(&name)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get label ID by name
  #[napi]
  pub fn get_label_id(&self, name: String) -> Result<Option<u32>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_label_id(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_label_id(&name)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get label name by ID
  #[napi]
  pub fn get_label_name(&self, id: u32) -> Result<Option<String>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_label_name(id)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_label_name(id)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get or create an edge type ID
  #[napi]
  pub fn get_or_create_etype(&self, name: String) -> Result<u32> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_or_create_etype(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_or_create_etype(&name)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get edge type ID by name
  #[napi]
  pub fn get_etype_id(&self, name: String) -> Result<Option<u32>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_etype_id(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_etype_id(&name)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get edge type name by ID
  #[napi]
  pub fn get_etype_name(&self, id: u32) -> Result<Option<String>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_etype_name(id)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_etype_name(id)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get or create a property key ID
  #[napi]
  pub fn get_or_create_propkey(&self, name: String) -> Result<u32> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_or_create_propkey(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_or_create_propkey(&name)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get property key ID by name
  #[napi]
  pub fn get_propkey_id(&self, name: String) -> Result<Option<u32>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_propkey_id(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_propkey_id(&name)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get property key name by ID
  #[napi]
  pub fn get_propkey_name(&self, id: u32) -> Result<Option<String>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_propkey_name(id)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_propkey_name(id)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Node Label Operations
  // ========================================================================

  /// Define a new label (requires transaction)
  #[napi]
  pub fn define_label(&self, name: String) -> Result<u32> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .define_label(&name)
        .map_err(|e| Error::from_reason(format!("Failed to define label: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        let label_id = graph_define_label(handle, &name)
          .map_err(|e| Error::from_reason(format!("Failed to define label: {e}")))?;
        Ok(label_id)
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Add a label to a node
  #[napi]
  pub fn add_node_label(&self, node_id: i64, label_id: u32) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .add_node_label(node_id as NodeId, label_id)
        .map_err(|e| Error::from_reason(format!("Failed to add label: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        graph_add_node_label(handle, node_id as NodeId, label_id)
          .map_err(|e| Error::from_reason(format!("Failed to add label: {e}")))?;
        Ok(())
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Add a label to a node by name
  #[napi]
  pub fn add_node_label_by_name(&self, node_id: i64, label_name: String) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .add_node_label_by_name(node_id as NodeId, &label_name)
        .map_err(|e| Error::from_reason(format!("Failed to add label: {e}"))),
      Some(DatabaseInner::Graph(db)) => {
        let label_id = db
          .get_label_id(&label_name)
          .ok_or_else(|| Error::from_reason(format!("Unknown label: {label_name}")))?;
        self.with_graph_tx(|handle| {
          graph_add_node_label(handle, node_id as NodeId, label_id)
            .map_err(|e| Error::from_reason(format!("Failed to add label: {e}")))?;
          Ok(())
        })
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Remove a label from a node
  #[napi]
  pub fn remove_node_label(&self, node_id: i64, label_id: u32) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .remove_node_label(node_id as NodeId, label_id)
        .map_err(|e| Error::from_reason(format!("Failed to remove label: {e}"))),
      Some(DatabaseInner::Graph(_)) => self.with_graph_tx(|handle| {
        graph_remove_node_label(handle, node_id as NodeId, label_id)
          .map_err(|e| Error::from_reason(format!("Failed to remove label: {e}")))?;
        Ok(())
      }),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Check if a node has a label
  #[napi]
  pub fn node_has_label(&self, node_id: i64, label_id: u32) -> Result<bool> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.node_has_label(node_id as NodeId, label_id)),
      Some(DatabaseInner::Graph(db)) => Ok(node_has_label_db(db, node_id as NodeId, label_id)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get all labels for a node
  #[napi]
  pub fn get_node_labels(&self, node_id: i64) -> Result<Vec<u32>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_node_labels(node_id as NodeId)),
      Some(DatabaseInner::Graph(db)) => Ok(get_node_labels_db(db, node_id as NodeId)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Graph Traversal (DB-backed)
  // ========================================================================

  /// Execute a single-hop traversal from start nodes
  ///
  /// @param startNodes - Array of starting node IDs
  /// @param direction - Traversal direction
  /// @param edgeType - Optional edge type filter
  /// @returns Array of traversal results
  #[napi]
  pub fn traverse_single(
    &self,
    start_nodes: Vec<i64>,
    direction: JsTraversalDirection,
    edge_type: Option<u32>,
  ) -> Result<Vec<JsTraversalResult>> {
    let start: Vec<NodeId> = start_nodes.iter().map(|&id| id as NodeId).collect();
    let etype = edge_type;

    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let builder = match direction {
          JsTraversalDirection::Out => RustTraversalBuilder::new(start).out(etype),
          JsTraversalDirection::In => RustTraversalBuilder::new(start).r#in(etype),
          JsTraversalDirection::Both => RustTraversalBuilder::new(start).both(etype),
        };

        Ok(
          builder
            .execute(|node_id, dir, etype| get_neighbors_from_single_file(db, node_id, dir, etype))
            .map(JsTraversalResult::from)
            .collect(),
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let builder = match direction {
          JsTraversalDirection::Out => RustTraversalBuilder::new(start).out(etype),
          JsTraversalDirection::In => RustTraversalBuilder::new(start).r#in(etype),
          JsTraversalDirection::Both => RustTraversalBuilder::new(start).both(etype),
        };

        Ok(
          builder
            .execute(|node_id, dir, etype| get_neighbors_from_graph_db(db, node_id, dir, etype))
            .map(JsTraversalResult::from)
            .collect(),
        )
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Execute a multi-hop traversal
  ///
  /// @param startNodes - Array of starting node IDs
  /// @param steps - Array of traversal steps (direction, edgeType)
  /// @param limit - Maximum number of results
  /// @returns Array of traversal results
  #[napi]
  pub fn traverse(
    &self,
    start_nodes: Vec<i64>,
    steps: Vec<JsTraversalStep>,
    limit: Option<u32>,
  ) -> Result<Vec<JsTraversalResult>> {
    let start: Vec<NodeId> = start_nodes.iter().map(|&id| id as NodeId).collect();
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let mut builder = RustTraversalBuilder::new(start);

        for step in steps {
          let etype = step.edge_type;
          builder = match step.direction {
            JsTraversalDirection::Out => builder.out(etype),
            JsTraversalDirection::In => builder.r#in(etype),
            JsTraversalDirection::Both => builder.both(etype),
          };
        }

        if let Some(n) = limit {
          builder = builder.take(n as usize);
        }

        Ok(
          builder
            .execute(|node_id, dir, etype| get_neighbors_from_single_file(db, node_id, dir, etype))
            .map(JsTraversalResult::from)
            .collect(),
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let mut builder = RustTraversalBuilder::new(start);

        for step in steps {
          let etype = step.edge_type;
          builder = match step.direction {
            JsTraversalDirection::Out => builder.out(etype),
            JsTraversalDirection::In => builder.r#in(etype),
            JsTraversalDirection::Both => builder.both(etype),
          };
        }

        if let Some(n) = limit {
          builder = builder.take(n as usize);
        }

        Ok(
          builder
            .execute(|node_id, dir, etype| get_neighbors_from_graph_db(db, node_id, dir, etype))
            .map(JsTraversalResult::from)
            .collect(),
        )
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Execute a variable-depth traversal
  ///
  /// @param startNodes - Array of starting node IDs
  /// @param edgeType - Optional edge type filter
  /// @param options - Traversal options (maxDepth, minDepth, direction, unique)
  /// @returns Array of traversal results
  #[napi]
  pub fn traverse_depth(
    &self,
    start_nodes: Vec<i64>,
    edge_type: Option<u32>,
    options: JsTraverseOptions,
  ) -> Result<Vec<JsTraversalResult>> {
    let start: Vec<NodeId> = start_nodes.iter().map(|&id| id as NodeId).collect();
    let opts: TraverseOptions = options.into();

    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        RustTraversalBuilder::new(start)
          .traverse(edge_type, opts)
          .execute(|node_id, dir, etype| get_neighbors_from_single_file(db, node_id, dir, etype))
          .map(JsTraversalResult::from)
          .collect(),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        RustTraversalBuilder::new(start)
          .traverse(edge_type, opts)
          .execute(|node_id, dir, etype| get_neighbors_from_graph_db(db, node_id, dir, etype))
          .map(JsTraversalResult::from)
          .collect(),
      ),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Count traversal results without materializing them
  ///
  /// @param startNodes - Array of starting node IDs
  /// @param steps - Array of traversal steps
  /// @returns Number of results
  #[napi]
  pub fn traverse_count(&self, start_nodes: Vec<i64>, steps: Vec<JsTraversalStep>) -> Result<u32> {
    let start: Vec<NodeId> = start_nodes.iter().map(|&id| id as NodeId).collect();
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let mut builder = RustTraversalBuilder::new(start);

        for step in steps {
          let etype = step.edge_type;
          builder = match step.direction {
            JsTraversalDirection::Out => builder.out(etype),
            JsTraversalDirection::In => builder.r#in(etype),
            JsTraversalDirection::Both => builder.both(etype),
          };
        }

        Ok(
          builder
            .count(|node_id, dir, etype| get_neighbors_from_single_file(db, node_id, dir, etype))
            as u32,
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let mut builder = RustTraversalBuilder::new(start);

        for step in steps {
          let etype = step.edge_type;
          builder = match step.direction {
            JsTraversalDirection::Out => builder.out(etype),
            JsTraversalDirection::In => builder.r#in(etype),
            JsTraversalDirection::Both => builder.both(etype),
          };
        }

        Ok(
          builder.count(|node_id, dir, etype| get_neighbors_from_graph_db(db, node_id, dir, etype))
            as u32,
        )
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get just the node IDs from a traversal
  ///
  /// @param startNodes - Array of starting node IDs
  /// @param steps - Array of traversal steps
  /// @param limit - Maximum number of results
  /// @returns Array of node IDs
  #[napi]
  pub fn traverse_node_ids(
    &self,
    start_nodes: Vec<i64>,
    steps: Vec<JsTraversalStep>,
    limit: Option<u32>,
  ) -> Result<Vec<i64>> {
    let start: Vec<NodeId> = start_nodes.iter().map(|&id| id as NodeId).collect();
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let mut builder = RustTraversalBuilder::new(start);

        for step in steps {
          let etype = step.edge_type;
          builder = match step.direction {
            JsTraversalDirection::Out => builder.out(etype),
            JsTraversalDirection::In => builder.r#in(etype),
            JsTraversalDirection::Both => builder.both(etype),
          };
        }

        if let Some(n) = limit {
          builder = builder.take(n as usize);
        }

        Ok(
          builder
            .collect_node_ids(|node_id, dir, etype| {
              get_neighbors_from_single_file(db, node_id, dir, etype)
            })
            .into_iter()
            .map(|id| id as i64)
            .collect(),
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let mut builder = RustTraversalBuilder::new(start);

        for step in steps {
          let etype = step.edge_type;
          builder = match step.direction {
            JsTraversalDirection::Out => builder.out(etype),
            JsTraversalDirection::In => builder.r#in(etype),
            JsTraversalDirection::Both => builder.both(etype),
          };
        }

        if let Some(n) = limit {
          builder = builder.take(n as usize);
        }

        Ok(
          builder
            .collect_node_ids(|node_id, dir, etype| {
              get_neighbors_from_graph_db(db, node_id, dir, etype)
            })
            .into_iter()
            .map(|id| id as i64)
            .collect(),
        )
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Pathfinding (DB-backed)
  // ========================================================================

  /// Find shortest path using Dijkstra's algorithm
  ///
  /// @param config - Pathfinding configuration
  /// @returns Path result with nodes, edges, and weight
  #[napi]
  pub fn dijkstra(&self, config: JsPathConfig) -> Result<JsPathResult> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let weight_key = resolve_weight_key_single_file(db, &config)?;
        let rust_config: PathConfig = config.into();
        Ok(
          dijkstra(
            rust_config,
            |node_id, dir, etype| get_neighbors_from_single_file(db, node_id, dir, etype),
            |src, etype, dst| get_edge_weight_from_single_file(db, src, etype, dst, weight_key),
          )
          .into(),
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let weight_key = resolve_weight_key_graph(db, &config)?;
        let rust_config: PathConfig = config.into();
        Ok(
          dijkstra(
            rust_config,
            |node_id, dir, etype| get_neighbors_from_graph_db(db, node_id, dir, etype),
            |src, etype, dst| get_edge_weight_from_graph_db(db, src, etype, dst, weight_key),
          )
          .into(),
        )
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Find shortest path using BFS (unweighted)
  ///
  /// Faster than Dijkstra for unweighted graphs.
  ///
  /// @param config - Pathfinding configuration
  /// @returns Path result with nodes, edges, and weight
  #[napi]
  pub fn bfs(&self, config: JsPathConfig) -> Result<JsPathResult> {
    let rust_config: PathConfig = config.into();
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        bfs(rust_config, |node_id, dir, etype| {
          get_neighbors_from_single_file(db, node_id, dir, etype)
        })
        .into(),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        bfs(rust_config, |node_id, dir, etype| {
          get_neighbors_from_graph_db(db, node_id, dir, etype)
        })
        .into(),
      ),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Find k shortest paths using Yen's algorithm
  ///
  /// @param config - Pathfinding configuration
  /// @param k - Maximum number of paths to find
  /// @returns Array of path results sorted by weight
  #[napi]
  pub fn k_shortest(&self, config: JsPathConfig, k: u32) -> Result<Vec<JsPathResult>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let weight_key = resolve_weight_key_single_file(db, &config)?;
        let rust_config: PathConfig = config.into();
        Ok(
          yen_k_shortest(
            rust_config,
            k as usize,
            |node_id, dir, etype| get_neighbors_from_single_file(db, node_id, dir, etype),
            |src, etype, dst| get_edge_weight_from_single_file(db, src, etype, dst, weight_key),
          )
          .into_iter()
          .map(JsPathResult::from)
          .collect(),
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let weight_key = resolve_weight_key_graph(db, &config)?;
        let rust_config: PathConfig = config.into();
        Ok(
          yen_k_shortest(
            rust_config,
            k as usize,
            |node_id, dir, etype| get_neighbors_from_graph_db(db, node_id, dir, etype),
            |src, etype, dst| get_edge_weight_from_graph_db(db, src, etype, dst, weight_key),
          )
          .into_iter()
          .map(JsPathResult::from)
          .collect(),
        )
      }
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Find shortest path between two nodes (convenience method)
  ///
  /// @param source - Source node ID
  /// @param target - Target node ID
  /// @param edgeType - Optional edge type filter
  /// @param maxDepth - Maximum search depth
  /// @returns Path result
  #[napi]
  pub fn shortest_path(
    &self,
    source: i64,
    target: i64,
    edge_type: Option<u32>,
    max_depth: Option<u32>,
  ) -> Result<JsPathResult> {
    let config = JsPathConfig {
      source,
      target: Some(target),
      targets: None,
      allowed_edge_types: edge_type.map(|e| vec![e]),
      weight_key_id: None,
      weight_key_name: None,
      direction: Some(JsTraversalDirection::Out),
      max_depth,
    };

    self.dijkstra(config)
  }

  /// Check if a path exists between two nodes
  ///
  /// @param source - Source node ID
  /// @param target - Target node ID
  /// @param edgeType - Optional edge type filter
  /// @param maxDepth - Maximum search depth
  /// @returns true if path exists
  #[napi]
  pub fn has_path(
    &self,
    source: i64,
    target: i64,
    edge_type: Option<u32>,
    max_depth: Option<u32>,
  ) -> Result<bool> {
    Ok(
      self
        .shortest_path(source, target, edge_type, max_depth)?
        .found,
    )
  }

  /// Get all nodes reachable from a source within a certain depth
  ///
  /// @param source - Source node ID
  /// @param maxDepth - Maximum depth to traverse
  /// @param edgeType - Optional edge type filter
  /// @returns Array of reachable node IDs
  #[napi]
  pub fn reachable_nodes(
    &self,
    source: i64,
    max_depth: u32,
    edge_type: Option<u32>,
  ) -> Result<Vec<i64>> {
    let opts = JsTraverseOptions {
      direction: Some(JsTraversalDirection::Out),
      min_depth: Some(1),
      max_depth,
      unique: Some(true),
    };

    Ok(
      self
        .traverse_depth(vec![source], edge_type, opts)?
        .into_iter()
        .map(|r| r.node_id)
        .collect(),
    )
  }

  // ========================================================================
  // Checkpoint / Maintenance
  // ========================================================================

  /// Perform a checkpoint (compact WAL into snapshot)
  #[napi]
  pub fn checkpoint(&self) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .checkpoint()
        .map_err(|e| Error::from_reason(format!("Failed to checkpoint: {e}"))),
      Some(DatabaseInner::Graph(_)) => Err(Error::from_reason(
        "checkpoint() only supports single-file databases",
      )),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Perform a background (non-blocking) checkpoint
  #[napi]
  pub fn background_checkpoint(&self) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .background_checkpoint()
        .map_err(|e| Error::from_reason(format!("Failed to background checkpoint: {e}"))),
      Some(DatabaseInner::Graph(_)) => Err(Error::from_reason(
        "backgroundCheckpoint() only supports single-file databases",
      )),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Check if checkpoint is recommended
  #[napi]
  pub fn should_checkpoint(&self, threshold: Option<f64>) -> Result<bool> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.should_checkpoint(threshold.unwrap_or(0.8))),
      Some(DatabaseInner::Graph(_)) => Ok(false),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Optimize (compact) the database
  ///
  /// This is an alias for `checkpoint()` to match the TypeScript API.
  /// For single-file databases, optimization means merging the WAL into
  /// the snapshot, which reduces file size and improves read performance.
  #[napi]
  pub fn optimize(&mut self) -> Result<()> {
    match self.inner.as_mut() {
      Some(DatabaseInner::SingleFile(db)) => db
        .checkpoint()
        .map_err(|e| Error::from_reason(format!("Failed to optimize: {e}"))),
      Some(DatabaseInner::Graph(db)) => db
        .optimize()
        .map_err(|e| Error::from_reason(format!("Failed to optimize: {e}"))),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get database statistics
  #[napi]
  pub fn stats(&self) -> Result<DbStats> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let s = db.stats();
        Ok(DbStats {
          snapshot_gen: s.snapshot_gen as i64,
          snapshot_nodes: s.snapshot_nodes as i64,
          snapshot_edges: s.snapshot_edges as i64,
          snapshot_max_node_id: s.snapshot_max_node_id as i64,
          delta_nodes_created: s.delta_nodes_created as i64,
          delta_nodes_deleted: s.delta_nodes_deleted as i64,
          delta_edges_added: s.delta_edges_added as i64,
          delta_edges_deleted: s.delta_edges_deleted as i64,
          wal_bytes: s.wal_bytes as i64,
          recommend_compact: s.recommend_compact,
        })
      }
      Some(DatabaseInner::Graph(db)) => Ok(graph_stats(db)),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Check database integrity
  #[napi]
  pub fn check(&self) -> Result<CheckResult> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(CheckResult::from(db.check())),
      Some(DatabaseInner::Graph(db)) => Ok(CheckResult::from(graph_check(db))),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Export / Import
  // ========================================================================

  /// Export database to a JSON object
  #[napi]
  pub fn export_to_object(&self, options: Option<ExportOptions>) -> Result<serde_json::Value> {
    let opts = options.unwrap_or(ExportOptions {
      include_nodes: None,
      include_edges: None,
      include_schema: None,
      pretty: None,
    });
    let opts = opts.to_rust();

    let data = match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => ray_export::export_to_object_single(db, opts)
        .map_err(|e| Error::from_reason(e.to_string()))?,
      Some(DatabaseInner::Graph(db)) => ray_export::export_to_object_graph(db, opts)
        .map_err(|e| Error::from_reason(e.to_string()))?,
      None => return Err(Error::from_reason("Database is closed")),
    };

    serde_json::to_value(data).map_err(|e| Error::from_reason(e.to_string()))
  }

  /// Export database to a JSON file
  #[napi]
  pub fn export_to_json(
    &self,
    path: String,
    options: Option<ExportOptions>,
  ) -> Result<ExportResult> {
    let opts = options.unwrap_or(ExportOptions {
      include_nodes: None,
      include_edges: None,
      include_schema: None,
      pretty: None,
    });
    let rust_opts = opts.to_rust();

    let data = match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        ray_export::export_to_object_single(db, rust_opts.clone())
          .map_err(|e| Error::from_reason(e.to_string()))?
      }
      Some(DatabaseInner::Graph(db)) => ray_export::export_to_object_graph(db, rust_opts.clone())
        .map_err(|e| Error::from_reason(e.to_string()))?,
      None => return Err(Error::from_reason("Database is closed")),
    };

    let result = ray_export::export_to_json(&data, path, rust_opts.pretty)
      .map_err(|e| Error::from_reason(e.to_string()))?;
    Ok(ExportResult {
      node_count: result.node_count as i64,
      edge_count: result.edge_count as i64,
    })
  }

  /// Export database to JSONL
  #[napi]
  pub fn export_to_jsonl(
    &self,
    path: String,
    options: Option<ExportOptions>,
  ) -> Result<ExportResult> {
    let opts = options.unwrap_or(ExportOptions {
      include_nodes: None,
      include_edges: None,
      include_schema: None,
      pretty: None,
    });
    let rust_opts = opts.to_rust();

    let data = match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => ray_export::export_to_object_single(db, rust_opts)
        .map_err(|e| Error::from_reason(e.to_string()))?,
      Some(DatabaseInner::Graph(db)) => ray_export::export_to_object_graph(db, rust_opts)
        .map_err(|e| Error::from_reason(e.to_string()))?,
      None => return Err(Error::from_reason("Database is closed")),
    };

    let result =
      ray_export::export_to_jsonl(&data, path).map_err(|e| Error::from_reason(e.to_string()))?;
    Ok(ExportResult {
      node_count: result.node_count as i64,
      edge_count: result.edge_count as i64,
    })
  }

  /// Import database from a JSON object
  #[napi]
  pub fn import_from_object(
    &self,
    data: serde_json::Value,
    options: Option<ImportOptions>,
  ) -> Result<ImportResult> {
    let opts = options.unwrap_or(ImportOptions {
      skip_existing: None,
      batch_size: None,
    });
    let rust_opts = opts.to_rust();
    let parsed: ray_export::ExportedDatabase =
      serde_json::from_value(data).map_err(|e| Error::from_reason(e.to_string()))?;

    let result = match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        ray_export::import_from_object_single(db, &parsed, rust_opts)
          .map_err(|e| Error::from_reason(e.to_string()))?
      }
      Some(DatabaseInner::Graph(db)) => {
        ray_export::import_from_object_graph(db, &parsed, rust_opts)
          .map_err(|e| Error::from_reason(e.to_string()))?
      }
      None => return Err(Error::from_reason("Database is closed")),
    };

    Ok(ImportResult {
      node_count: result.node_count as i64,
      edge_count: result.edge_count as i64,
      skipped: result.skipped as i64,
    })
  }

  /// Import database from a JSON file
  #[napi]
  pub fn import_from_json(
    &self,
    path: String,
    options: Option<ImportOptions>,
  ) -> Result<ImportResult> {
    let opts = options.unwrap_or(ImportOptions {
      skip_existing: None,
      batch_size: None,
    });
    let rust_opts = opts.to_rust();
    let parsed =
      ray_export::import_from_json(path).map_err(|e| Error::from_reason(e.to_string()))?;

    let result = match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        ray_export::import_from_object_single(db, &parsed, rust_opts)
          .map_err(|e| Error::from_reason(e.to_string()))?
      }
      Some(DatabaseInner::Graph(db)) => {
        ray_export::import_from_object_graph(db, &parsed, rust_opts)
          .map_err(|e| Error::from_reason(e.to_string()))?
      }
      None => return Err(Error::from_reason("Database is closed")),
    };

    Ok(ImportResult {
      node_count: result.node_count as i64,
      edge_count: result.edge_count as i64,
      skipped: result.skipped as i64,
    })
  }

  // ========================================================================
  // Cache Operations
  // ========================================================================

  /// Check if caching is enabled
  #[napi]
  pub fn cache_is_enabled(&self) -> Result<bool> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.cache_is_enabled()),
      Some(DatabaseInner::Graph(_)) => Ok(false),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Invalidate all caches for a node
  #[napi]
  pub fn cache_invalidate_node(&self, node_id: i64) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_invalidate_node(node_id as NodeId);
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Invalidate caches for a specific edge
  #[napi]
  pub fn cache_invalidate_edge(&self, src: i64, etype: u32, dst: i64) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_invalidate_edge(src as NodeId, etype as ETypeId, dst as NodeId);
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Invalidate a cached key lookup
  #[napi]
  pub fn cache_invalidate_key(&self, key: String) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_invalidate_key(&key);
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Clear all caches
  #[napi]
  pub fn cache_clear(&self) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_clear();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Clear only the query cache
  #[napi]
  pub fn cache_clear_query(&self) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_clear_query();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Clear only the key cache
  #[napi]
  pub fn cache_clear_key(&self) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_clear_key();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Clear only the property cache
  #[napi]
  pub fn cache_clear_property(&self) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_clear_property();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Clear only the traversal cache
  #[napi]
  pub fn cache_clear_traversal(&self) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_clear_traversal();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Get cache statistics
  #[napi]
  pub fn cache_stats(&self) -> Result<Option<JsCacheStats>> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.cache_stats().map(|s| JsCacheStats {
        property_cache_hits: s.property_cache_hits as i64,
        property_cache_misses: s.property_cache_misses as i64,
        property_cache_size: s.property_cache_size as i64,
        traversal_cache_hits: s.traversal_cache_hits as i64,
        traversal_cache_misses: s.traversal_cache_misses as i64,
        traversal_cache_size: s.traversal_cache_size as i64,
        query_cache_hits: s.query_cache_hits as i64,
        query_cache_misses: s.query_cache_misses as i64,
        query_cache_size: s.query_cache_size as i64,
      })),
      Some(DatabaseInner::Graph(_)) => Ok(None),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  /// Reset cache statistics
  #[napi]
  pub fn cache_reset_stats(&self) -> Result<()> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_reset_stats();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  // ========================================================================
  // Internal Helpers
  // ========================================================================

  fn get_db(&self) -> Result<&RustSingleFileDB> {
    match self.inner.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db),
      Some(DatabaseInner::Graph(_)) => Err(Error::from_reason("Database is multi-file")),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  fn get_graph_db(&self) -> Result<&RustGraphDB> {
    match self.inner.as_ref() {
      Some(DatabaseInner::Graph(db)) => Ok(db),
      Some(DatabaseInner::SingleFile(_)) => Err(Error::from_reason("Database is single-file")),
      None => Err(Error::from_reason("Database is closed")),
    }
  }

  fn with_graph_tx<F, R>(&self, f: F) -> Result<R>
  where
    F: FnOnce(&mut GraphTxHandle) -> Result<R>,
  {
    let db = self.get_graph_db()?;
    let mut guard = self.graph_tx.lock();
    let tx_state = guard
      .take()
      .ok_or_else(|| Error::from_reason("No active transaction"))?;
    let mut handle = GraphTxHandle::new(db, tx_state);
    let result = f(&mut handle);
    *guard = Some(handle.tx);
    result
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Get neighbors from database for traversal
fn get_neighbors_from_single_file(
  db: &RustSingleFileDB,
  node_id: NodeId,
  direction: TraversalDirection,
  etype: Option<ETypeId>,
) -> Vec<Edge> {
  let mut edges = Vec::new();
  match direction {
    TraversalDirection::Out => {
      for (e, dst) in db.get_out_edges(node_id) {
        if etype.is_none() || etype == Some(e) {
          edges.push(Edge {
            src: node_id,
            etype: e,
            dst,
          });
        }
      }
    }
    TraversalDirection::In => {
      for (e, src) in db.get_in_edges(node_id) {
        if etype.is_none() || etype == Some(e) {
          edges.push(Edge {
            src,
            etype: e,
            dst: node_id,
          });
        }
      }
    }
    TraversalDirection::Both => {
      edges.extend(get_neighbors_from_single_file(
        db,
        node_id,
        TraversalDirection::Out,
        etype,
      ));
      edges.extend(get_neighbors_from_single_file(
        db,
        node_id,
        TraversalDirection::In,
        etype,
      ));
    }
  }
  edges
}

fn get_neighbors_from_graph_db(
  db: &RustGraphDB,
  node_id: NodeId,
  direction: TraversalDirection,
  etype: Option<ETypeId>,
) -> Vec<Edge> {
  let mut edges = Vec::new();
  match direction {
    TraversalDirection::Out => {
      for edge in list_out_edges(db, node_id) {
        if etype.is_none() || etype == Some(edge.etype) {
          edges.push(Edge {
            src: node_id,
            etype: edge.etype,
            dst: edge.dst,
          });
        }
      }
    }
    TraversalDirection::In => {
      for edge in list_in_edges(db, node_id) {
        if etype.is_none() || etype == Some(edge.etype) {
          edges.push(Edge {
            src: edge.dst,
            etype: edge.etype,
            dst: node_id,
          });
        }
      }
    }
    TraversalDirection::Both => {
      edges.extend(get_neighbors_from_graph_db(
        db,
        node_id,
        TraversalDirection::Out,
        etype,
      ));
      edges.extend(get_neighbors_from_graph_db(
        db,
        node_id,
        TraversalDirection::In,
        etype,
      ));
    }
  }
  edges
}

fn resolve_weight_key_single_file(
  db: &RustSingleFileDB,
  config: &JsPathConfig,
) -> Result<Option<PropKeyId>> {
  if let Some(key_id) = config.weight_key_id {
    return Ok(Some(key_id as PropKeyId));
  }

  if let Some(ref key_name) = config.weight_key_name {
    let key_id = db
      .get_propkey_id(key_name)
      .ok_or_else(|| Error::from_reason(format!("Unknown property key: {key_name}")))?;
    return Ok(Some(key_id));
  }

  Ok(None)
}

fn resolve_weight_key_graph(db: &RustGraphDB, config: &JsPathConfig) -> Result<Option<PropKeyId>> {
  if let Some(key_id) = config.weight_key_id {
    return Ok(Some(key_id as PropKeyId));
  }

  if let Some(ref key_name) = config.weight_key_name {
    let key_id = db
      .get_propkey_id(key_name)
      .ok_or_else(|| Error::from_reason(format!("Unknown property key: {key_name}")))?;
    return Ok(Some(key_id));
  }

  Ok(None)
}

fn prop_value_to_weight(value: Option<PropValue>) -> f64 {
  let weight = match value {
    Some(PropValue::Bool(v)) => {
      if v {
        1.0
      } else {
        0.0
      }
    }
    Some(PropValue::I64(v)) => v as f64,
    Some(PropValue::F64(v)) => v,
    Some(PropValue::String(v)) => v.parse::<f64>().unwrap_or(1.0),
    Some(PropValue::VectorF32(_)) => 1.0,
    Some(PropValue::Null) | None => 1.0,
  };

  if weight.is_finite() && weight > 0.0 {
    weight
  } else {
    1.0
  }
}

fn get_edge_weight_from_single_file(
  db: &RustSingleFileDB,
  src: NodeId,
  etype: ETypeId,
  dst: NodeId,
  weight_key: Option<PropKeyId>,
) -> f64 {
  match weight_key {
    Some(key_id) => prop_value_to_weight(db.get_edge_prop(src, etype, dst, key_id)),
    None => 1.0,
  }
}

fn get_edge_weight_from_graph_db(
  db: &RustGraphDB,
  src: NodeId,
  etype: ETypeId,
  dst: NodeId,
  weight_key: Option<PropKeyId>,
) -> f64 {
  match weight_key {
    Some(key_id) => prop_value_to_weight(get_edge_prop_db(db, src, etype, dst, key_id)),
    None => 1.0,
  }
}

fn graph_stats(db: &RustGraphDB) -> DbStats {
  let node_count = graph_count_nodes(db);
  let edge_count = graph_count_edges(db, None);

  let delta = db.delta.read();
  let delta_nodes_created = delta.created_nodes.len();
  let delta_nodes_deleted = delta.deleted_nodes.len();
  let delta_edges_added = delta.total_edges_added();
  let delta_edges_deleted = delta.total_edges_deleted();
  drop(delta);

  let (snapshot_gen, snapshot_nodes, snapshot_edges, snapshot_max_node_id) =
    if let Some(ref snapshot) = db.snapshot {
      (
        snapshot.header.generation,
        snapshot.header.num_nodes,
        snapshot.header.num_edges,
        snapshot.header.max_node_id,
      )
    } else {
      (0, 0, 0, 0)
    };

  let total_changes =
    delta_nodes_created + delta_nodes_deleted + delta_edges_added + delta_edges_deleted;
  let recommend_compact = total_changes > 10_000;

  DbStats {
    snapshot_gen: snapshot_gen as i64,
    snapshot_nodes: snapshot_nodes.max(node_count) as i64,
    snapshot_edges: snapshot_edges.max(edge_count) as i64,
    snapshot_max_node_id: snapshot_max_node_id as i64,
    delta_nodes_created: delta_nodes_created as i64,
    delta_nodes_deleted: delta_nodes_deleted as i64,
    delta_edges_added: delta_edges_added as i64,
    delta_edges_deleted: delta_edges_deleted as i64,
    wal_bytes: db.wal_bytes() as i64,
    recommend_compact,
  }
}

fn graph_check(db: &RustGraphDB) -> RustCheckResult {
  let mut errors = Vec::new();
  let mut warnings = Vec::new();

  let all_nodes = graph_list_nodes(db);
  let node_count = all_nodes.len();

  if node_count == 0 {
    warnings.push("No nodes in database".to_string());
    return RustCheckResult {
      valid: true,
      errors,
      warnings,
    };
  }

  let all_edges = graph_list_edges(db, ListEdgesOptions::default());
  let edge_count = all_edges.len();

  for edge in &all_edges {
    if !node_exists_db(db, edge.src) {
      errors.push(format!(
        "Edge references non-existent source node: {} -[{}]-> {}",
        edge.src, edge.etype, edge.dst
      ));
    }

    if !node_exists_db(db, edge.dst) {
      errors.push(format!(
        "Edge references non-existent destination node: {} -[{}]-> {}",
        edge.src, edge.etype, edge.dst
      ));
    }
  }

  for edge in &all_edges {
    let exists = edge_exists_db(db, edge.src, edge.etype, edge.dst);
    if !exists {
      errors.push(format!(
        "Edge inconsistency: edge {} -[{}]-> {} listed but not found via edge_exists",
        edge.src, edge.etype, edge.dst
      ));
    }
  }

  let counted_nodes = graph_count_nodes(db);
  let counted_edges = graph_count_edges(db, None);

  if counted_nodes as usize != node_count {
    warnings.push(format!(
      "Node count mismatch: list_nodes returned {node_count} but count_nodes returned {counted_nodes}"
    ));
  }

  if counted_edges as usize != edge_count {
    warnings.push(format!(
      "Edge count mismatch: list_edges returned {edge_count} but count_edges returned {counted_edges}"
    ));
  }

  RustCheckResult {
    valid: errors.is_empty(),
    errors,
    warnings,
  }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Open a database file (standalone function)
#[napi]
pub fn open_database(path: String, options: Option<OpenOptions>) -> Result<Database> {
  Database::open(path, options)
}

// ============================================================================
// Metrics / Health
// ============================================================================

#[napi]
pub fn collect_metrics(db: &Database) -> Result<DatabaseMetrics> {
  match db.inner.as_ref() {
    Some(DatabaseInner::SingleFile(db)) => Ok(collect_metrics_single_file(db)),
    Some(DatabaseInner::Graph(db)) => Ok(collect_metrics_graph(db)),
    None => Err(Error::from_reason("Database is closed")),
  }
}

#[napi]
pub fn health_check(db: &Database) -> Result<HealthCheckResult> {
  match db.inner.as_ref() {
    Some(DatabaseInner::SingleFile(db)) => Ok(health_check_single_file(db)),
    Some(DatabaseInner::Graph(db)) => Ok(health_check_graph(db)),
    None => Err(Error::from_reason("Database is closed")),
  }
}

fn calc_hit_rate(hits: u64, misses: u64) -> f64 {
  let total = hits + misses;
  if total > 0 {
    hits as f64 / total as f64
  } else {
    0.0
  }
}

fn build_cache_layer_metrics(
  hits: u64,
  misses: u64,
  size: usize,
  max_size: usize,
) -> CacheLayerMetrics {
  CacheLayerMetrics {
    hits: hits as i64,
    misses: misses as i64,
    hit_rate: calc_hit_rate(hits, misses),
    size: size as i64,
    max_size: max_size as i64,
    utilization_percent: if max_size > 0 {
      (size as f64 / max_size as f64) * 100.0
    } else {
      0.0
    },
  }
}

fn empty_cache_layer_metrics() -> CacheLayerMetrics {
  CacheLayerMetrics {
    hits: 0,
    misses: 0,
    hit_rate: 0.0,
    size: 0,
    max_size: 0,
    utilization_percent: 0.0,
  }
}

fn build_cache_metrics(stats: Option<&CacheManagerStats>) -> CacheMetrics {
  match stats {
    Some(stats) => CacheMetrics {
      enabled: true,
      property_cache: build_cache_layer_metrics(
        stats.property_cache_hits,
        stats.property_cache_misses,
        stats.property_cache_size,
        stats.property_cache_max_size,
      ),
      traversal_cache: build_cache_layer_metrics(
        stats.traversal_cache_hits,
        stats.traversal_cache_misses,
        stats.traversal_cache_size,
        stats.traversal_cache_max_size,
      ),
      query_cache: build_cache_layer_metrics(
        stats.query_cache_hits,
        stats.query_cache_misses,
        stats.query_cache_size,
        stats.query_cache_max_size,
      ),
    },
    None => CacheMetrics {
      enabled: false,
      property_cache: empty_cache_layer_metrics(),
      traversal_cache: empty_cache_layer_metrics(),
      query_cache: empty_cache_layer_metrics(),
    },
  }
}

fn estimate_delta_memory(delta: &DeltaState) -> i64 {
  let mut bytes = 0i64;

  bytes += delta.created_nodes.len() as i64 * 100;
  bytes += delta.deleted_nodes.len() as i64 * 8;
  bytes += delta.modified_nodes.len() as i64 * 100;

  for patches in delta.out_add.values() {
    bytes += patches.len() as i64 * 24;
  }
  for patches in delta.out_del.values() {
    bytes += patches.len() as i64 * 24;
  }
  for patches in delta.in_add.values() {
    bytes += patches.len() as i64 * 24;
  }
  for patches in delta.in_del.values() {
    bytes += patches.len() as i64 * 24;
  }

  bytes += delta.edge_props.len() as i64 * 50;
  bytes += delta.key_index.len() as i64 * 40;

  bytes
}

fn estimate_cache_memory(stats: Option<&CacheManagerStats>) -> i64 {
  match stats {
    Some(stats) => {
      (stats.property_cache_size as i64 * 100)
        + (stats.traversal_cache_size as i64 * 200)
        + (stats.query_cache_size as i64 * 500)
    }
    None => 0,
  }
}

fn delta_health_size(delta: &DeltaState) -> usize {
  delta.created_nodes.len()
    + delta.deleted_nodes.len()
    + delta.modified_nodes.len()
    + delta.out_add.len()
    + delta.in_add.len()
}

fn collect_metrics_single_file(db: &RustSingleFileDB) -> DatabaseMetrics {
  let stats = db.stats();
  let delta = db.delta.read();
  let cache_stats = db.cache.read().as_ref().map(|cache| cache.stats());

  let node_count = stats.snapshot_nodes as i64 + stats.delta_nodes_created as i64
    - stats.delta_nodes_deleted as i64;
  let edge_count =
    stats.snapshot_edges as i64 + stats.delta_edges_added as i64 - stats.delta_edges_deleted as i64;

  let data = DataMetrics {
    node_count,
    edge_count,
    delta_nodes_created: stats.delta_nodes_created as i64,
    delta_nodes_deleted: stats.delta_nodes_deleted as i64,
    delta_edges_added: stats.delta_edges_added as i64,
    delta_edges_deleted: stats.delta_edges_deleted as i64,
    snapshot_generation: stats.snapshot_gen as i64,
    max_node_id: stats.snapshot_max_node_id as i64,
    schema_labels: delta.new_labels.len() as i64,
    schema_etypes: delta.new_etypes.len() as i64,
    schema_prop_keys: delta.new_propkeys.len() as i64,
  };

  let cache = build_cache_metrics(cache_stats.as_ref());
  let delta_bytes = estimate_delta_memory(&delta);
  let cache_bytes = estimate_cache_memory(cache_stats.as_ref());
  let snapshot_bytes = (stats.snapshot_nodes as i64 * 50) + (stats.snapshot_edges as i64 * 20);

  DatabaseMetrics {
    path: db.path.to_string_lossy().to_string(),
    is_single_file: true,
    read_only: db.read_only,
    data,
    cache,
    mvcc: None,
    memory: MemoryMetrics {
      delta_estimate_bytes: delta_bytes,
      cache_estimate_bytes: cache_bytes,
      snapshot_bytes,
      total_estimate_bytes: delta_bytes + cache_bytes + snapshot_bytes,
    },
    collected_at: system_time_to_millis(SystemTime::now()),
  }
}

fn collect_metrics_graph(db: &RustGraphDB) -> DatabaseMetrics {
  let stats = graph_stats(db);
  let delta = db.delta.read();

  let node_count = stats.snapshot_nodes + stats.delta_nodes_created - stats.delta_nodes_deleted;
  let edge_count = stats.snapshot_edges + stats.delta_edges_added - stats.delta_edges_deleted;

  let data = DataMetrics {
    node_count,
    edge_count,
    delta_nodes_created: stats.delta_nodes_created,
    delta_nodes_deleted: stats.delta_nodes_deleted,
    delta_edges_added: stats.delta_edges_added,
    delta_edges_deleted: stats.delta_edges_deleted,
    snapshot_generation: stats.snapshot_gen,
    max_node_id: stats.snapshot_max_node_id,
    schema_labels: delta.new_labels.len() as i64,
    schema_etypes: delta.new_etypes.len() as i64,
    schema_prop_keys: delta.new_propkeys.len() as i64,
  };

  let cache = build_cache_metrics(None);
  let delta_bytes = estimate_delta_memory(&delta);
  let snapshot_bytes = (stats.snapshot_nodes * 50) + (stats.snapshot_edges * 20);

  DatabaseMetrics {
    path: db.path.to_string_lossy().to_string(),
    is_single_file: false,
    read_only: db.read_only,
    data,
    cache,
    mvcc: None,
    memory: MemoryMetrics {
      delta_estimate_bytes: delta_bytes,
      cache_estimate_bytes: 0,
      snapshot_bytes,
      total_estimate_bytes: delta_bytes + snapshot_bytes,
    },
    collected_at: system_time_to_millis(SystemTime::now()),
  }
}

fn health_check_single_file(db: &RustSingleFileDB) -> HealthCheckResult {
  let mut checks = Vec::new();

  checks.push(HealthCheckEntry {
    name: "database_open".to_string(),
    passed: true,
    message: "Database handle is valid".to_string(),
  });

  let delta = db.delta.read();
  let delta_size = delta_health_size(&delta);
  let delta_ok = delta_size < 100000;
  checks.push(HealthCheckEntry {
    name: "delta_size".to_string(),
    passed: delta_ok,
    message: if delta_ok {
      format!("Delta size is reasonable ({delta_size} entries)")
    } else {
      format!("Delta is large ({delta_size} entries) - consider checkpointing")
    },
  });

  let cache_stats = db.cache.read().as_ref().map(|cache| cache.stats());
  if let Some(stats) = cache_stats {
    let total_hits = stats.property_cache_hits + stats.traversal_cache_hits;
    let total_misses = stats.property_cache_misses + stats.traversal_cache_misses;
    let total = total_hits + total_misses;
    let hit_rate = if total > 0 {
      total_hits as f64 / total as f64
    } else {
      1.0
    };
    let cache_ok = hit_rate > 0.5 || total < 100;
    checks.push(HealthCheckEntry {
      name: "cache_efficiency".to_string(),
      passed: cache_ok,
      message: if cache_ok {
        format!("Cache hit rate: {:.1}%", hit_rate * 100.0)
      } else {
        format!(
          "Low cache hit rate: {:.1}% - consider adjusting cache size",
          hit_rate * 100.0
        )
      },
    });
  }

  if db.read_only {
    checks.push(HealthCheckEntry {
      name: "write_access".to_string(),
      passed: true,
      message: "Database is read-only".to_string(),
    });
  }

  let healthy = checks.iter().all(|check| check.passed);
  HealthCheckResult { healthy, checks }
}

fn health_check_graph(db: &RustGraphDB) -> HealthCheckResult {
  let mut checks = Vec::new();

  checks.push(HealthCheckEntry {
    name: "database_open".to_string(),
    passed: true,
    message: "Database handle is valid".to_string(),
  });

  let delta = db.delta.read();
  let delta_size = delta_health_size(&delta);
  let delta_ok = delta_size < 100000;
  checks.push(HealthCheckEntry {
    name: "delta_size".to_string(),
    passed: delta_ok,
    message: if delta_ok {
      format!("Delta size is reasonable ({delta_size} entries)")
    } else {
      format!("Delta is large ({delta_size} entries) - consider checkpointing")
    },
  });

  if db.read_only {
    checks.push(HealthCheckEntry {
      name: "write_access".to_string(),
      passed: true,
      message: "Database is read-only".to_string(),
    });
  }

  let healthy = checks.iter().all(|check| check.passed);
  HealthCheckResult { healthy, checks }
}

// ============================================================================
// Backup / Restore
// ============================================================================

/// Options for creating a backup
#[napi(object)]
#[derive(Default)]
pub struct BackupOptions {
  /// Force a checkpoint before backup (single-file only)
  pub checkpoint: Option<bool>,
  /// Overwrite existing backup if it exists
  pub overwrite: Option<bool>,
}

/// Options for restoring a backup
#[napi(object)]
#[derive(Default)]
pub struct RestoreOptions {
  /// Overwrite existing database if it exists
  pub overwrite: Option<bool>,
}

/// Options for offline backup
#[napi(object)]
#[derive(Default)]
pub struct OfflineBackupOptions {
  /// Overwrite existing backup if it exists
  pub overwrite: Option<bool>,
}

/// Backup result
#[napi(object)]
pub struct BackupResult {
  /// Backup path
  pub path: String,
  /// Size in bytes
  pub size: i64,
  /// Timestamp in milliseconds since epoch
  pub timestamp: i64,
  /// Backup type ("single-file" or "multi-file")
  pub r#type: String,
}

fn system_time_to_millis(time: SystemTime) -> i64 {
  match time.duration_since(UNIX_EPOCH) {
    Ok(duration) => duration.as_millis() as i64,
    Err(_) => 0,
  }
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
  if let Some(parent) = path.parent() {
    if !parent.as_os_str().is_empty() {
      fs::create_dir_all(parent).map_err(|e| Error::from_reason(e.to_string()))?;
    }
  }
  Ok(())
}

fn remove_existing(path: &Path) -> Result<()> {
  if path.is_dir() {
    fs::remove_dir_all(path).map_err(|e| Error::from_reason(e.to_string()))?;
  } else {
    fs::remove_file(path).map_err(|e| Error::from_reason(e.to_string()))?;
  }
  Ok(())
}

fn copy_file_with_size(src: &Path, dst: &Path) -> Result<u64> {
  fs::copy(src, dst).map_err(|e| Error::from_reason(e.to_string()))?;
  let size = fs::metadata(src)
    .map_err(|e| Error::from_reason(e.to_string()))?
    .len();
  Ok(size)
}

fn dir_size(path: &Path) -> Result<u64> {
  let mut total = 0u64;
  for entry in fs::read_dir(path).map_err(|e| Error::from_reason(e.to_string()))? {
    let entry = entry.map_err(|e| Error::from_reason(e.to_string()))?;
    let entry_path = entry.path();
    let metadata = entry
      .metadata()
      .map_err(|e| Error::from_reason(e.to_string()))?;
    if metadata.is_dir() {
      total += dir_size(&entry_path)?;
    } else {
      total += metadata.len();
    }
  }
  Ok(total)
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<u64> {
  fs::create_dir_all(dst).map_err(|e| Error::from_reason(e.to_string()))?;
  let mut total = 0u64;
  for entry in fs::read_dir(src).map_err(|e| Error::from_reason(e.to_string()))? {
    let entry = entry.map_err(|e| Error::from_reason(e.to_string()))?;
    let src_path = entry.path();
    let dst_path = dst.join(entry.file_name());
    let metadata = entry
      .metadata()
      .map_err(|e| Error::from_reason(e.to_string()))?;
    if metadata.is_dir() {
      total += copy_dir_recursive(&src_path, &dst_path)?;
    } else {
      total += copy_file_with_size(&src_path, &dst_path)?;
    }
  }
  Ok(total)
}

fn backup_result(path: &Path, size: u64, kind: &str, timestamp: SystemTime) -> BackupResult {
  BackupResult {
    path: path.to_string_lossy().to_string(),
    size: size as i64,
    timestamp: system_time_to_millis(timestamp),
    r#type: kind.to_string(),
  }
}

/// Create a backup from an open database handle
#[napi]
pub fn create_backup(
  db: &Database,
  backup_path: String,
  options: Option<BackupOptions>,
) -> Result<BackupResult> {
  let options = options.unwrap_or_default();
  let do_checkpoint = options.checkpoint.unwrap_or(true);
  let overwrite = options.overwrite.unwrap_or(false);
  let mut backup_path = PathBuf::from(backup_path);

  if backup_path.exists() && !overwrite {
    return Err(Error::from_reason(
      "Backup already exists at path (use overwrite: true)".to_string(),
    ));
  }

  match db.inner.as_ref() {
    Some(DatabaseInner::SingleFile(db)) => {
      if !backup_path.to_string_lossy().ends_with(EXT_RAYDB) {
        backup_path = PathBuf::from(format!("{}{}", backup_path.to_string_lossy(), EXT_RAYDB));
      }

      if do_checkpoint && !db.read_only {
        db.checkpoint()
          .map_err(|e| Error::from_reason(format!("Failed to checkpoint: {e}")))?;
      }

      ensure_parent_dir(&backup_path)?;

      if overwrite && backup_path.exists() {
        remove_existing(&backup_path)?;
      }

      copy_file_with_size(&db.path, &backup_path)?;
      let size = fs::metadata(&backup_path)
        .map_err(|e| Error::from_reason(e.to_string()))?
        .len();

      Ok(backup_result(
        &backup_path,
        size,
        "single-file",
        SystemTime::now(),
      ))
    }
    Some(DatabaseInner::Graph(db)) => {
      if overwrite && backup_path.exists() {
        remove_existing(&backup_path)?;
      }

      fs::create_dir_all(&backup_path).map_err(|e| Error::from_reason(e.to_string()))?;
      fs::create_dir_all(backup_path.join(SNAPSHOTS_DIR))
        .map_err(|e| Error::from_reason(e.to_string()))?;
      fs::create_dir_all(backup_path.join(WAL_DIR))
        .map_err(|e| Error::from_reason(e.to_string()))?;

      let mut total_size = 0u64;
      let manifest_src = db.path.join(MANIFEST_FILENAME);
      if manifest_src.exists() {
        total_size += copy_file_with_size(&manifest_src, &backup_path.join(MANIFEST_FILENAME))?;
      }

      let snapshots_dir = db.path.join(SNAPSHOTS_DIR);
      if snapshots_dir.exists() {
        for entry in fs::read_dir(&snapshots_dir).map_err(|e| Error::from_reason(e.to_string()))? {
          let entry = entry.map_err(|e| Error::from_reason(e.to_string()))?;
          let src = entry.path();
          if entry
            .file_type()
            .map_err(|e| Error::from_reason(e.to_string()))?
            .is_file()
          {
            let dst = backup_path.join(SNAPSHOTS_DIR).join(entry.file_name());
            total_size += copy_file_with_size(&src, &dst)?;
          }
        }
      }

      let wal_dir = db.path.join(WAL_DIR);
      if wal_dir.exists() {
        for entry in fs::read_dir(&wal_dir).map_err(|e| Error::from_reason(e.to_string()))? {
          let entry = entry.map_err(|e| Error::from_reason(e.to_string()))?;
          let src = entry.path();
          if entry
            .file_type()
            .map_err(|e| Error::from_reason(e.to_string()))?
            .is_file()
          {
            let dst = backup_path.join(WAL_DIR).join(entry.file_name());
            total_size += copy_file_with_size(&src, &dst)?;
          }
        }
      }

      Ok(backup_result(
        &backup_path,
        total_size,
        "multi-file",
        SystemTime::now(),
      ))
    }
    None => Err(Error::from_reason("Database is closed")),
  }
}

/// Restore a backup into a target path
#[napi]
pub fn restore_backup(
  backup_path: String,
  restore_path: String,
  options: Option<RestoreOptions>,
) -> Result<String> {
  let options = options.unwrap_or_default();
  let overwrite = options.overwrite.unwrap_or(false);
  let backup_path = PathBuf::from(backup_path);
  let mut restore_path = PathBuf::from(restore_path);

  if !backup_path.exists() {
    return Err(Error::from_reason("Backup not found at path".to_string()));
  }

  if restore_path.exists() && !overwrite {
    return Err(Error::from_reason(
      "Database already exists at restore path (use overwrite: true)".to_string(),
    ));
  }

  let metadata = fs::metadata(&backup_path).map_err(|e| Error::from_reason(e.to_string()))?;
  if metadata.is_file() {
    if !restore_path.to_string_lossy().ends_with(EXT_RAYDB) {
      restore_path = PathBuf::from(format!("{}{}", restore_path.to_string_lossy(), EXT_RAYDB));
    }

    ensure_parent_dir(&restore_path)?;

    if overwrite && restore_path.exists() {
      remove_existing(&restore_path)?;
    }

    copy_file_with_size(&backup_path, &restore_path)?;
    Ok(restore_path.to_string_lossy().to_string())
  } else if metadata.is_dir() {
    if overwrite && restore_path.exists() {
      remove_existing(&restore_path)?;
    }

    fs::create_dir_all(&restore_path).map_err(|e| Error::from_reason(e.to_string()))?;
    fs::create_dir_all(restore_path.join(SNAPSHOTS_DIR))
      .map_err(|e| Error::from_reason(e.to_string()))?;
    fs::create_dir_all(restore_path.join(WAL_DIR))
      .map_err(|e| Error::from_reason(e.to_string()))?;

    let manifest_src = backup_path.join(MANIFEST_FILENAME);
    if manifest_src.exists() {
      copy_file_with_size(&manifest_src, &restore_path.join(MANIFEST_FILENAME))?;
    }

    let snapshots_dir = backup_path.join(SNAPSHOTS_DIR);
    if snapshots_dir.exists() {
      for entry in fs::read_dir(&snapshots_dir).map_err(|e| Error::from_reason(e.to_string()))? {
        let entry = entry.map_err(|e| Error::from_reason(e.to_string()))?;
        let src = entry.path();
        if entry
          .file_type()
          .map_err(|e| Error::from_reason(e.to_string()))?
          .is_file()
        {
          let dst = restore_path.join(SNAPSHOTS_DIR).join(entry.file_name());
          copy_file_with_size(&src, &dst)?;
        }
      }
    }

    let wal_dir = backup_path.join(WAL_DIR);
    if wal_dir.exists() {
      for entry in fs::read_dir(&wal_dir).map_err(|e| Error::from_reason(e.to_string()))? {
        let entry = entry.map_err(|e| Error::from_reason(e.to_string()))?;
        let src = entry.path();
        if entry
          .file_type()
          .map_err(|e| Error::from_reason(e.to_string()))?
          .is_file()
        {
          let dst = restore_path.join(WAL_DIR).join(entry.file_name());
          copy_file_with_size(&src, &dst)?;
        }
      }
    }

    Ok(restore_path.to_string_lossy().to_string())
  } else {
    Err(Error::from_reason(
      "Backup path is not a file or directory".to_string(),
    ))
  }
}

/// Inspect a backup without restoring it
#[napi]
pub fn get_backup_info(backup_path: String) -> Result<BackupResult> {
  let backup_path = PathBuf::from(backup_path);
  if !backup_path.exists() {
    return Err(Error::from_reason("Backup not found at path".to_string()));
  }

  let metadata = fs::metadata(&backup_path).map_err(|e| Error::from_reason(e.to_string()))?;
  let timestamp = metadata.modified().unwrap_or_else(|_| SystemTime::now());

  if metadata.is_file() {
    Ok(backup_result(
      &backup_path,
      metadata.len(),
      "single-file",
      timestamp,
    ))
  } else if metadata.is_dir() {
    let size = dir_size(&backup_path)?;
    Ok(backup_result(&backup_path, size, "multi-file", timestamp))
  } else {
    Err(Error::from_reason(
      "Backup path is not a file or directory".to_string(),
    ))
  }
}

/// Create a backup from a database path without opening it
#[napi]
pub fn create_offline_backup(
  db_path: String,
  backup_path: String,
  options: Option<OfflineBackupOptions>,
) -> Result<BackupResult> {
  let options = options.unwrap_or_default();
  let overwrite = options.overwrite.unwrap_or(false);
  let db_path = PathBuf::from(db_path);
  let backup_path = PathBuf::from(backup_path);

  if !db_path.exists() {
    return Err(Error::from_reason("Database not found at path".to_string()));
  }

  if backup_path.exists() && !overwrite {
    return Err(Error::from_reason(
      "Backup already exists at path (use overwrite: true)".to_string(),
    ));
  }

  let metadata = fs::metadata(&db_path).map_err(|e| Error::from_reason(e.to_string()))?;
  if metadata.is_file() {
    ensure_parent_dir(&backup_path)?;
    if overwrite && backup_path.exists() {
      remove_existing(&backup_path)?;
    }
    copy_file_with_size(&db_path, &backup_path)?;
    let size = fs::metadata(&backup_path)
      .map_err(|e| Error::from_reason(e.to_string()))?
      .len();
    Ok(backup_result(
      &backup_path,
      size,
      "single-file",
      SystemTime::now(),
    ))
  } else if metadata.is_dir() {
    if overwrite && backup_path.exists() {
      remove_existing(&backup_path)?;
    }
    let size = copy_dir_recursive(&db_path, &backup_path)?;
    Ok(backup_result(
      &backup_path,
      size,
      "multi-file",
      SystemTime::now(),
    ))
  } else {
    Err(Error::from_reason(
      "Database path is not a file or directory".to_string(),
    ))
  }
}
