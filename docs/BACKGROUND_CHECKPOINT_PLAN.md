# Background Checkpointing for Write Performance

## Problem Summary

Single-file format write performance degrades at larger batch sizes due to **auto-checkpoint latency spikes**:

| Batch Size | Expected | Actual | Issue |
|------------|----------|--------|-------|
| 10 nodes | Fast | 150K nodes/sec | OK |
| 100 nodes | Faster | 529K nodes/sec | OK |
| 1000 nodes | Fastest | **95K nodes/sec** | 5x slower than 100-node batches |

**Root cause**: When WAL reaches 80% capacity, auto-checkpoint triggers a full compaction (30-60ms+), blocking the commit. This causes p95/max latency spikes that destroy throughput for larger batches.

```
Batch 0:  1.5ms, WAL=4.6%
Batch 17: 36.6ms, WAL=4.6%  <- Checkpoint triggered
Batch 34: 64.7ms, WAL=4.6%  <- Checkpoint triggered
```

## Goal

Achieve consistent, high-throughput writes that scale linearly with batch size:
- Target: **800K+ nodes/sec** for large batches (matching multi-file performance)
- Eliminate checkpoint latency spikes from the write path
- Maintain durability guarantees

---

## Architecture: Dual-Buffer WAL with Background Checkpoint

```
+------------------------------------------------------------------+
|                        Single-File DB                            |
+----------+----------------------+--------------------------------+
| Header   |      WAL Area        |         Snapshot               |
|  (V2)    | +--------+----------+|                                |
|          | |Primary |Secondary ||                                |
|          | | (75%)  |  (25%)   ||                                |
|          | +--------+----------+|                                |
+----------+----------------------+--------------------------------+
```

**Normal operation**: Writes go to primary WAL (75% of space)

**During checkpoint**: 
1. Writes switch to secondary WAL (25% of space)
2. Background task reads primary WAL + snapshot, builds new snapshot
3. On completion: merge secondary -> new primary, swap snapshot, reset

---

## Implementation Phases

### Phase 1: Header V2 & WAL Split

**Files:**
- `src/types.ts`
- `src/core/header.ts`
- `src/core/wal-buffer.ts`

**Changes:**

1. Add new header fields:
   ```typescript
   interface DbHeaderV2 {
     // ... existing V1 fields ...
     walPrimaryHead: bigint;       // Primary region write position
     walSecondaryHead: bigint;     // Secondary region write position
     activeWalRegion: 0 | 1;       // 0=primary, 1=secondary
     checkpointInProgress: 0 | 1;  // Crash recovery flag
   }
   ```

2. Update `WalBuffer` class:
   - Split WAL 75/25 (primary/secondary)
   - Add `switchToSecondary()`, `mergeSecondaryIntoPrimary()`
   - Add `scanRegion(region)` for targeted reads
   - Track which region is active for writes

---

### Phase 2: Background Checkpoint

**Files:**
- `src/ray/graph-db/checkpoint.ts`
- `src/ray/graph-db/tx.ts`

**Changes:**

1. Add checkpoint state tracking:
   ```typescript
   type CheckpointState = 
     | { status: 'idle' }
     | { status: 'running'; promise: Promise<void> }
     | { status: 'completing' };
   ```

2. Make checkpoint trigger non-blocking in `commitSingleFile()`:
   - Write to active WAL region
   - If threshold reached, call `triggerBackgroundCheckpoint(db)` (returns immediately)
   - Continue with commit

3. Implement `backgroundCheckpoint()`:
   - Switch writes to secondary
   - Set `checkpointInProgress = 1` in header
   - Read primary WAL + snapshot (consistent view)
   - Build new snapshot in memory
   - Write new snapshot to disk
   - Brief lock: merge secondary -> primary, update header, swap mmap
   - Set `checkpointInProgress = 0`

---

### Phase 3: Backpressure

**Files:**
- `src/ray/graph-db/tx.ts`

**Changes:**

1. In `commitSingleFile()`, before writing:
   ```typescript
   if (checkpointRunning && secondaryUsage > 0.9) {
     await checkpointPromise; // Wait for checkpoint
   }
   ```

2. This ensures writes only block when absolutely necessary (secondary WAL nearly full).

---

### Phase 4: Crash Recovery

**Files:**
- `src/ray/graph-db/single-file.ts`
- `src/ray/graph-db/wal-replay.ts`

**Changes:**

1. On open, check `checkpointInProgress` flag:
   - If set, replay both primary and secondary WAL regions
   - Clear flag after recovery

2. Recovery matrix:
   | Flag | Active Region | Action |
   |------|---------------|--------|
   | 0 | 0 (primary) | Normal replay of primary |
   | 1 | 1 (secondary) | Replay primary + secondary |

---

### Phase 5: Testing

**New file:** `tests/background-checkpoint.test.ts`

**Test cases:**
1. Writes continue during checkpoint (no blocking)
2. Backpressure triggers at 90% secondary usage
3. Crash recovery with `checkpointInProgress = 1`
4. Crash recovery with data in both WAL regions
5. Multiple checkpoints in sequence
6. Checkpoint completes before secondary fills

---

### Phase 6: Benchmark Updates

**File:** `bench/benchmark-single-file.ts`

**Changes:**
- Track checkpoint events separately from write latency
- Report: checkpoint count, avg duration, writes during checkpoint
- Verify 1000-node batch achieves 800K+ nodes/sec

---

## Task Breakdown

| # | Task | Files | Est. |
|---|------|-------|------|
| 1 | Add header V2 fields to types | `src/types.ts` | 10 min |
| 2 | Update header serialization/parsing | `src/core/header.ts` | 20 min |
| 3 | Implement dual-region WalBuffer | `src/core/wal-buffer.ts` | 45 min |
| 4 | Add checkpoint state to GraphDB type | `src/types.ts` | 5 min |
| 5 | Implement background checkpoint | `src/ray/graph-db/checkpoint.ts` | 45 min |
| 6 | Update commit for non-blocking trigger | `src/ray/graph-db/tx.ts` | 20 min |
| 7 | Add backpressure logic | `src/ray/graph-db/tx.ts` | 15 min |
| 8 | Update crash recovery | `src/ray/graph-db/single-file.ts` | 30 min |
| 9 | Update WAL replay for dual-region | `src/ray/graph-db/wal-replay.ts` | 20 min |
| 10 | Write background checkpoint tests | `tests/background-checkpoint.test.ts` | 40 min |
| 11 | Update benchmark with checkpoint metrics | `bench/benchmark-single-file.ts` | 15 min |
| 12 | Run full test suite + benchmarks | - | 10 min |

**Total estimated time:** ~4.5 hours

---

## Expected Outcome

**Before:**
```
Batch of 1000 nodes    p50=1.2ms  p95=65ms   (95K nodes/sec)
                                   ^ checkpoint spike
```

**After:**
```
Batch of 1000 nodes    p50=1.2ms  p95=1.8ms  (800K nodes/sec)
Checkpoints: 3 @ avg 45ms (0 writes blocked)
```

---

## Design Decisions

1. **Header versioning**: Bumping to V2 (no backwards compatibility required)

2. **WAL split ratio**: 75/25 (primary/secondary) - secondary only accumulates during checkpoint, so needs less space

3. **Async mechanism**: Single-threaded async (`queueMicrotask`/`setTimeout(0)`) - checkpoint is I/O bound, true parallelism not needed initially

4. **Backpressure threshold**: 90% secondary WAL usage triggers blocking wait for checkpoint completion
