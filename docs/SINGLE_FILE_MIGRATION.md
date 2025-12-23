# Single-File Database Format: API Integration Plan

## Overview

This document outlines the plan to complete the single-file `.raydb` format integration with the high-level Ray API. The storage layer is fully implemented; this plan focuses on connecting it to the API layer.

---

## Current Implementation Status

### COMPLETE ✓ (Storage Layer)

| Component | File | Status |
|-----------|------|--------|
| Database Header | `src/core/header.ts` | Full header structure, serialization, checksums |
| Page-based I/O | `src/core/pager.ts` | `FilePager` with mmap, allocation, lock byte protection |
| Circular WAL Buffer | `src/core/wal-buffer.ts` | `WalBuffer` with wrap-around, skip markers |
| Single-File Lifecycle | `src/ray/graph-db/single-file.ts` | `openSingleFileDB`, `closeSingleFileDB`, WAL recovery |
| Compaction | `src/core/single-file-compactor.ts` | `optimizeSingleFile`, `vacuumSingleFile` |
| Tests | `tests/single-file.test.ts` | 900+ lines covering storage layer |

### INCOMPLETE ✗ (API Layer)

| Component | Issue |
|-----------|-------|
| Transaction Layer | `beginTx`/`commit` in `tx.ts` assume multi-file format |
| Database Open | `openGraphDB` doesn't dispatch to single-file |
| Interface Compatibility | `GraphDB` and `SingleFileDB` are separate, incompatible interfaces |
| High-Level API | `ray()` function cannot use `.raydb` files |

---

## Design Decisions

1. **Internal-only single-file exports**: `openSingleFileDB`/`closeSingleFileDB` become internal implementation details
2. **Cached SnapshotData**: Parse and cache on first access, with opt-out via `cacheSnapshot: false`
3. **Auto-checkpoint with escape hatch**: Automatically checkpoint when WAL buffer reaches threshold (default 80%), disable with `autoCheckpoint: false`
4. **Single-file as default**: New databases use `.raydb` format; paths auto-append `.raydb` extension
5. **Backward compatibility for existing directories**: Detect `manifest.gdm` and use multi-file format for existing databases

---

## File Format Reference

```
┌─────────────────────────────────────────────────┐
│                  HEADER (4KB)                    │
│  Magic, version, page size, WAL/snapshot ptrs    │
├─────────────────────────────────────────────────┤
│               WAL AREA (circular)                │
│  Default 64MB, configurable                      │
├─────────────────────────────────────────────────┤
│              SNAPSHOT AREA (mmap'd)              │
│  CSR format, 64-byte aligned sections            │
├─────────────────────────────────────────────────┤
│               FREE SPACE / GROWTH                │
└─────────────────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: Unified Database Interface

**Goal**: Merge `GraphDB` and `SingleFileDB` into a single interface.

| Task | Description | Files |
|------|-------------|-------|
| 1.1 | Add `_isSingleFile: boolean` discriminator to `GraphDB` | `src/types.ts` |
| 1.2 | Add optional single-file fields to `GraphDB`: `_header`, `_pager`, `_snapshotMmap`, `_walWritePos` | `src/types.ts` |
| 1.3 | Make multi-file fields optional: `_manifest`, `_snapshot`, `_walFd`, `_walOffset` | `src/types.ts` |
| 1.4 | Delete `SingleFileDB` interface | `src/types.ts` |
| 1.5 | Update `openSingleFileDB` to return `GraphDB` with `_isSingleFile: true` | `src/ray/graph-db/single-file.ts` |
| 1.6 | Update `openGraphDB` to detect path and dispatch appropriately | `src/ray/graph-db/lifecycle.ts` |
| 1.7 | Update `closeGraphDB` to handle both formats | `src/ray/graph-db/lifecycle.ts` |
| 1.8 | Remove separate single-file exports from public API | `src/index.ts` |

**New `GraphDB` interface structure:**

```typescript
export interface GraphDB {
  readonly path: string;
  readonly readOnly: boolean;
  readonly _isSingleFile: boolean;
  
  // Multi-file specific (null for single-file)
  _manifest: ManifestV1 | null;
  _snapshot: SnapshotData | null;
  _walFd: number | null;
  _walOffset: number;
  
  // Single-file specific (null for multi-file)
  _header: DbHeaderV1 | null;
  _pager: Pager | null;
  _snapshotMmap: Uint8Array | null;
  _snapshotCache: SnapshotData | null;  // Cached parsed snapshot
  _walWritePos: number;
  
  // Shared fields
  _delta: DeltaState;
  _nextNodeId: number;
  _nextLabelId: number;
  _nextEtypeId: number;
  _nextPropkeyId: number;
  _nextTxId: bigint;
  _currentTx: TxState | null;
  _lockFd: unknown;
  _cache?: CacheManager;
  _mvcc?: MvccManager;
  _mvccEnabled?: boolean;
  
  // Options
  _autoCheckpoint?: boolean;
  _checkpointThreshold?: number;
  _cacheSnapshot?: boolean;
}
```

---

### Phase 2: Transaction Layer for Single-File

**Goal**: Make `beginTx`/`commit` work with single-file format.

| Task | Description | Files |
|------|-------------|-------|
| 2.1 | Extract `buildCommitRecords(db, tx): WalRecord[]` helper | `src/ray/graph-db/tx.ts` |
| 2.2 | Extract `applyToDelta(db, tx): void` helper | `src/ray/graph-db/tx.ts` |
| 2.3 | Implement `commitSingleFile(db, records)` that writes to `WalBuffer` | `src/ray/graph-db/tx.ts` |
| 2.4 | Update `commit()` to dispatch based on `db._isSingleFile` | `src/ray/graph-db/tx.ts` |
| 2.5 | Verify `rollback()` works correctly (should work as-is) | `src/ray/graph-db/tx.ts` |

**Single-file commit flow:**

```typescript
async function commitSingleFile(db: GraphDB, records: WalRecord[]): Promise<void> {
  const pager = db._pager as FilePager;
  const walBuffer = new WalBuffer(pager, db._header!);
  
  // Check if auto-checkpoint needed before writing
  if (db._autoCheckpoint && shouldCheckpoint(db, records)) {
    await checkpoint(db);
  }
  
  // Write records to circular buffer
  for (const record of records) {
    walBuffer.writeRecord(record);
  }
  
  // Update header with new WAL head
  const newHeader = updateHeaderForCommit(
    db._header!,
    walBuffer.getHead(),
    BigInt(db._nextNodeId - 1),
    db._nextTxId,
  );
  await writeHeader(pager, newHeader);
  await pager.sync();
  
  db._header = newHeader;
  db._walWritePos = Number(walBuffer.getHead());
}
```

---

### Phase 3: Snapshot Access & Caching

**Goal**: Unify snapshot access for both formats with optional caching.

| Task | Description | Files |
|------|-------------|-------|
| 3.1 | Create `getSnapshot(db): SnapshotData \| null` helper | `src/ray/graph-db/snapshot-helper.ts` (new) |
| 3.2 | Implement lazy parsing with caching for single-file | `src/ray/graph-db/snapshot-helper.ts` |
| 3.3 | Add `cacheSnapshot` option to `OpenOptions` (default: `true`) | `src/types.ts` |
| 3.4 | Update `nodes.ts` to use `getSnapshot()` | `src/ray/graph-db/nodes.ts` |
| 3.5 | Update `edges.ts` to use `getSnapshot()` | `src/ray/graph-db/edges.ts` |
| 3.6 | Update `key-index.ts` to use `getSnapshot()` | `src/ray/key-index.ts` |
| 3.7 | Update `stats.ts` to use `getSnapshot()` | `src/ray/graph-db/stats.ts` |

**Snapshot helper implementation:**

```typescript
// src/ray/graph-db/snapshot-helper.ts
export function getSnapshot(db: GraphDB): SnapshotData | null {
  if (!db._isSingleFile) {
    return db._snapshot;
  }
  
  // Single-file: use cache if available
  if (db._snapshotCache) {
    return db._snapshotCache;
  }
  
  // Parse from mmap if available
  if (!db._snapshotMmap) {
    return null;
  }
  
  const snapshot = parseSnapshot(db._snapshotMmap);
  
  // Cache if enabled
  if (db._cacheSnapshot !== false) {
    db._snapshotCache = snapshot;
  }
  
  return snapshot;
}

export function invalidateSnapshotCache(db: GraphDB): void {
  if (db._isSingleFile) {
    db._snapshotCache = null;
  }
}
```

---

### Phase 4: Auto-Checkpoint Implementation

**Goal**: Automatically checkpoint when WAL buffer reaches threshold.

| Task | Description | Files |
|------|-------------|-------|
| 4.1 | Add `autoCheckpoint` option to `OpenOptions` (default: `true`) | `src/types.ts` |
| 4.2 | Add `checkpointThreshold` option (default: `0.8`) | `src/types.ts` |
| 4.3 | Create `shouldCheckpoint(db, pendingRecords): boolean` helper | `src/ray/graph-db/checkpoint.ts` (new) |
| 4.4 | Create `checkpoint(db): Promise<void>` that calls `optimizeSingleFile` | `src/ray/graph-db/checkpoint.ts` |
| 4.5 | Integrate checkpoint into `commitSingleFile()` | `src/ray/graph-db/tx.ts` |
| 4.6 | Invalidate `_snapshotCache` after checkpoint | `src/ray/graph-db/checkpoint.ts` |
| 4.7 | Re-mmap snapshot area after checkpoint | `src/ray/graph-db/checkpoint.ts` |

**Checkpoint logic:**

```typescript
// src/ray/graph-db/checkpoint.ts
export function shouldCheckpoint(db: GraphDB, pendingRecords: WalRecord[]): boolean {
  if (!db._isSingleFile || !db._autoCheckpoint) {
    return false;
  }
  
  const pager = db._pager as FilePager;
  const walBuffer = new WalBuffer(pager, db._header!);
  
  // Estimate size of pending records
  let pendingSize = 0;
  for (const record of pendingRecords) {
    pendingSize += buildWalRecord(record).length;
  }
  
  const usedAfterCommit = walBuffer.getUsedSpace() + pendingSize;
  const threshold = db._checkpointThreshold ?? 0.8;
  const walSize = Number(db._header!.walPageCount) * db._header!.pageSize;
  
  return usedAfterCommit / walSize >= threshold;
}

export async function checkpoint(db: GraphDB): Promise<void> {
  if (!db._isSingleFile) {
    throw new Error("checkpoint() only works with single-file databases");
  }
  
  await optimizeSingleFile(db as any);
  
  // Invalidate snapshot cache (new snapshot created)
  invalidateSnapshotCache(db);
  
  // Re-mmap the new snapshot area
  const pager = db._pager as FilePager;
  if (db._header!.snapshotPageCount > 0n) {
    db._snapshotMmap = pager.mmapRange(
      Number(db._header!.snapshotStartPage),
      Number(db._header!.snapshotPageCount)
    );
  }
}
```

---

### Phase 5: Path Handling & Format Detection

**Goal**: Auto-append `.raydb` extension, detect existing directory databases.

| Task | Description | Files |
|------|-------------|-------|
| 5.1 | In `openGraphDB`, auto-append `.raydb` if path has no extension | `src/ray/graph-db/lifecycle.ts` |
| 5.2 | Detect existing directory format by checking for `manifest.gdm` | `src/ray/graph-db/lifecycle.ts` |
| 5.3 | If directory format detected, use existing multi-file logic with `_isSingleFile: false` | `src/ray/graph-db/lifecycle.ts` |
| 5.4 | Update `closeGraphDB` to check `_isSingleFile` and dispatch | `src/ray/graph-db/lifecycle.ts` |

**Path handling logic:**

```typescript
export async function openGraphDB(path: string, options: OpenOptions = {}): Promise<GraphDB> {
  // Check if it's an existing directory database
  const manifestPath = join(path, MANIFEST_FILENAME);
  if (existsSync(manifestPath)) {
    // Existing directory format - use multi-file logic
    return openMultiFileDB(path, options);
  }
  
  // Check if path already ends with .raydb
  let dbPath = path;
  if (!path.endsWith(EXT_RAYDB)) {
    dbPath = path + EXT_RAYDB;
  }
  
  // Single-file format
  return openSingleFileDB(dbPath, options);
}
```

---

### Phase 6: Testing

**Goal**: Ensure everything works end-to-end.

| Task | Description | Files |
|------|-------------|-------|
| 6.1 | Create `tests/ray-single-file.integration.test.ts` | New file |
| 6.2 | Test: create database with Ray API, verify `.raydb` file created | Test file |
| 6.3 | Test: insert nodes/edges, close, reopen, verify data persists | Test file |
| 6.4 | Test: transactions commit correctly | Test file |
| 6.5 | Test: auto-checkpoint triggers at threshold | Test file |
| 6.6 | Test: `autoCheckpoint: false` throws `WalBufferFullError` | Test file |
| 6.7 | Test: existing directory databases still work | Test file |
| 6.8 | Test: `cacheSnapshot: false` works correctly | Test file |
| 6.9 | Update `tests/single-file.test.ts` to use unified API where appropriate | Existing file |

---

### Phase 7: Cleanup & Documentation

**Goal**: Remove obsolete code and update docs.

| Task | Description | Files |
|------|-------------|-------|
| 7.1 | Remove `SingleFileDB` exports from `src/index.ts` | `src/index.ts` |
| 7.2 | Update `playground/generate-auth-db.ts` to use Ray API | `playground/generate-auth-db.ts` |
| 7.3 | Verify playground can load generated `auth.raydb` | Manual test |
| 7.4 | Update README with single-file format info | `README.md` |

---

## New Configuration Options

```typescript
interface OpenOptions {
  // Existing options...
  readOnly?: boolean;
  createIfMissing?: boolean;
  lockFile?: boolean;
  
  // New single-file options
  autoCheckpoint?: boolean;      // Default: true - auto-checkpoint when WAL fills
  checkpointThreshold?: number;  // Default: 0.8 - trigger checkpoint at 80% WAL usage
  cacheSnapshot?: boolean;       // Default: true - cache parsed snapshot in memory
  
  // Single-file creation options
  pageSize?: number;             // Default: 4096 - page size for new databases
  walSize?: number;              // Default: 64MB - WAL area size
}
```

---

## Estimated Effort

| Phase | Tasks | Time |
|-------|-------|------|
| Phase 1: Unified Interface | 8 | 2-3 hours |
| Phase 2: Transaction Layer | 5 | 3-4 hours |
| Phase 3: Snapshot Access | 7 | 2 hours |
| Phase 4: Auto-Checkpoint | 7 | 2 hours |
| Phase 5: Path Handling | 4 | 1 hour |
| Phase 6: Testing | 9 | 3 hours |
| Phase 7: Cleanup | 4 | 1 hour |
| **Total** | **44** | **~14-16 hours** |

---

## Migration Notes

### For Existing Directory Databases

Existing databases using the directory format will continue to work. The system detects them by the presence of `manifest.gdm` and uses the multi-file code path.

A future `migrateToSingleFile(path)` utility could be added to convert directory databases to single-file format.

### Breaking Changes

- `openSingleFileDB` and `closeSingleFileDB` are no longer exported (internal only)
- `SingleFileDB` type is removed (use `GraphDB` with `_isSingleFile: true`)
- New databases default to single-file format (`.raydb` extension auto-appended)
