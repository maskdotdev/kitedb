# Bindings Parity Checklist

Sources
- TypeScript API docs: `docs/api/README.md`
- TypeScript exports: `src/index.ts`
- NAPI surface: `ray-rs/index.d.ts`
- Python bindings: `ray-rs/python/raydb/__init__.py`, `ray-rs/src/pyo3_bindings/*.rs`

Legend
- Full: Parity with TS feature and behavior
- Partial: Subset of functionality or different semantics
- Missing: Not exposed in bindings

## Parity Matrix

| Feature area | TypeScript | Python | NAPI | Notes |
| --- | --- | --- | --- | --- |
| DB open/close (auto-detect dir + .raydb) | Full | Missing | Full | Python only opens single-file via `Database` |
| Single-file DB open/close | Full | Full | Full | `Database`/`open_database`/`openDatabase` |
| Single-file open options | Full | Full | Full | Page size, WAL size, cache, sync mode |
| MVCC option | Full | Missing | Partial | NAPI supports `mvcc` flag only |
| Locking utility | Full | Missing | Missing | `isProperLockingAvailable` |
| Backup/restore | Full | Missing | Full | `createBackup`, `restoreBackup`, `createOfflineBackup` |
| Export/import | Full | Missing | Partial | JSON export/import + JSONL export; no JSONL import |
| Streaming/pagination | Full | Missing | Full | `streamNodes`, `getNodesPage` |
| Metrics/health | Full | Missing | Full | `collectMetrics`, `healthCheck` (timestamp is epoch ms) |
| Low-level transactions | Full | Partial | Partial | Single-file only |
| Low-level node CRUD | Full | Full | Full | `createNode`, `deleteNode`, `getNodeByKey`, `listNodes`, `countNodes` |
| Low-level edge CRUD | Full | Full | Full | `addEdge`, `deleteEdge`, `listEdges`, `countEdges` |
| Properties (node/edge) | Full | Full | Full | `set/get/del` node/edge props |
| Vector PropValue | Full | Missing | Full | Python bindings do not expose PropValue VectorF32 |
| Schema IDs/labels | Full | Full | Full | Labels, edge types, prop keys |
| Cache API | Full | Full | Full | Python/NAPI include extra cache control |
| Integrity check | Full | Missing | Partial | NAPI runs graph-level checks (not snapshot checker) |
| Optimize/compact | Full | Partial | Partial | Checkpoint/optimize only; no vacuum/options |
| Vector embeddings | Full | Full | Full | `set/get/del/has` node vectors |
| Vector IVF/IVF-PQ + brute force | Full | Full | Full | Index classes + brute force search |
| VectorIndex (high-level) | Full | Missing | Partial | NAPI returns node IDs (no NodeRef cache yet) |
| Fluent API (Ray) | Full | Partial | Partial | NAPI supports schema input + CRUD + traversal + pathfinding |
| Fluent traversal helpers | Full | Full | Missing | Python includes `where_edge`, `edges()`, `raw_edges()`, `select()`, `traverse()` |
| Fluent pathfinding | Full | Full | Missing | Python includes BFS/Dijkstra/A*, weights, to_any/all_paths |
| DB-backed traversal/pathfinding (low-level) | Full | Partial | Partial | Python has traverse + BFS/Dijkstra; NAPI adds DB traversal + BFS/Dijkstra/K-shortest with optional weightKeyId/Name |

## Language-Specific Gaps

Python gaps (bindings + fluent layer)
- Missing multi-file DB open/close (dir format) and auto-detect open
- Missing `check()` integrity API
- Missing high-level `VectorIndex` API
- No export/import, backup/restore, streaming/pagination, metrics/health
- PropValue vector type not supported (vectors handled only via `set_node_vector`)

NAPI gaps
- No JSONL import for export/import

## Doc mismatches to fix
- None currently tracked.

## Implementation backlog (draft)

Python
- [ ] Add multi-file DB open/close (match `openGraphDB`) and auto-detect
- [ ] Expose `check()` integrity verification
- [ ] Add high-level `VectorIndex` API (Python equivalent of TS `VectorIndex`)
- [ ] Expose export/import and backup/restore
- [ ] Expose streaming/pagination helpers
- [ ] Expose metrics/health

NAPI
- [x] Add high-level fluent API surface (Ray + schema + builders) (partial)
- [x] Add multi-file DB open/close with auto-detect
- [x] Expose `check()` integrity verification
- [x] Add high-level `VectorIndex` API
- [x] Expose export/import (JSON + JSONL export only)
- [x] Expose backup/restore
- [x] Expose streaming/pagination helpers
- [x] Expose metrics/health
