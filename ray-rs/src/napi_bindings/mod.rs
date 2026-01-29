//! NAPI bindings for RayDB
//!
//! Exposes SingleFileDB and related types to Node.js/Bun.

pub mod database;
pub mod ray;
pub mod traversal;
pub mod vector;

pub use database::{
  collect_metrics, create_backup, create_offline_backup, get_backup_info, health_check,
  open_database, restore_backup, BackupOptions, BackupResult, CacheLayerMetrics, CacheMetrics,
  CheckResult, DataMetrics, Database, DatabaseMetrics, DbStats, EdgePage, EdgeWithProps,
  HealthCheckEntry, HealthCheckResult, JsEdge, JsFullEdge, JsNodeProp, JsPropValue, MemoryMetrics,
  MvccMetrics, NodePage, NodeWithProps, OfflineBackupOptions, OpenOptions, PaginationOptions,
  PropType, RestoreOptions, StreamOptions,
};

pub use ray::{
  ray, JsEdgeSpec, JsKeySpec, JsNodeSpec, JsPathEdge, JsPathResult, JsPropSpec, JsRayOptions, Ray,
  RayInsertBuilder, RayInsertExecutorMany, RayInsertExecutorSingle, RayPath, RayTraversal,
  RayUpdateBuilder,
};

pub use traversal::{
  path_config, traversal_step, JsEdgeInput, JsGraphAccessor, JsPathConfig, JsTraversalDirection,
  JsTraversalResult, JsTraversalStep, JsTraverseOptions,
};

pub use vector::{
  brute_force_search, create_vector_index, JsAggregation, JsBruteForceResult, JsDistanceMetric,
  JsIvfConfig, JsIvfIndex, JsIvfPqIndex, JsIvfStats, JsPqConfig, JsSearchOptions, JsSearchResult,
  SimilarOptions, VectorIndex, VectorIndexOptions, VectorIndexStats, VectorSearchHit,
};
