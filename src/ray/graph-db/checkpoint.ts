/**
 * Checkpoint management for single-file databases
 * Auto-checkpoint when WAL buffer reaches threshold
 * 
 * Supports two modes:
 * 1. Blocking checkpoint: Traditional approach, blocks writes during compaction
 * 2. Background checkpoint: Non-blocking, writes continue to secondary WAL region
 */

import type { CheckpointState, GraphDB } from "../../types.ts";
import type { WalRecord } from "../../core/wal.ts";
import { estimateWalRecordSize } from "../../core/wal.ts";
import { createWalBuffer, WalBuffer } from "../../core/wal-buffer.ts";
import type { FilePager } from "../../core/pager.ts";
import { optimizeSingleFile } from "../../core/single-file-compactor.ts";
import { invalidateSnapshotCache } from "./snapshot-helper.ts";
import { parseSnapshot } from "../../core/snapshot-reader.ts";
import { writeHeader, updateHeaderForCompaction } from "../../core/header.ts";
import { clearDelta } from "../../core/delta.ts";
import { pagesToStore } from "../../core/pager.ts";
import {
  type EdgeData,
  type NodeData,
  buildSnapshotBuffer,
} from "../../core/snapshot-writer-buffer.ts";
import { collectGraphDataForCheckpoint } from "./snapshot-helper.ts";

/**
 * Check if auto-checkpoint should be triggered based on primary WAL usage
 * Returns true if primary region usage exceeds threshold
 */
export function shouldCheckpoint(db: GraphDB, pendingRecords: WalRecord[]): boolean {
  if (!db._isSingleFile || !db._autoCheckpoint) {
    return false;
  }
  
  if (!db._pager || !db._header) {
    return false;
  }
  
  const pager = db._pager as FilePager;
  const walBuffer = createWalBuffer(pager, db._header);
  
  // Estimate size of pending records without building them
  // This avoids double memory allocation and CRC computation
  let pendingSize = 0;
  for (const record of pendingRecords) {
    pendingSize += estimateWalRecordSize(record);
  }
  
  // Use primary region size for threshold calculation
  const primaryRegionSize = walBuffer.getPrimaryRegionSize();
  const currentUsage = walBuffer.getUsedSpace() + pendingSize;
  const threshold = db._checkpointThreshold ?? 0.8;
  
  return currentUsage / primaryRegionSize >= threshold;
}

/**
 * Get current checkpoint state (or initialize to idle)
 */
export function getCheckpointState(db: GraphDB): CheckpointState {
  if (!db._checkpointState) {
    db._checkpointState = { status: 'idle' };
  }
  return db._checkpointState;
}

/**
 * Check if a background checkpoint is currently running
 */
export function isCheckpointRunning(db: GraphDB): boolean {
  const state = getCheckpointState(db);
  return state.status === 'running' || state.status === 'completing';
}

/**
 * Get the checkpoint promise if one is running
 */
export function getCheckpointPromise(db: GraphDB): Promise<void> | null {
  const state = getCheckpointState(db);
  if (state.status === 'running') {
    return state.promise;
  }
  return null;
}

/**
 * Trigger a background checkpoint (non-blocking)
 * 
 * This switches writes to secondary region immediately (synchronously updates in-memory header)
 * and then starts the checkpoint process asynchronously.
 * 
 * @returns Promise that resolves when checkpoint completes (optional await)
 */
export function triggerBackgroundCheckpoint(db: GraphDB): Promise<void> {
  if (!db._isSingleFile) {
    throw new Error("triggerBackgroundCheckpoint() only works with single-file databases");
  }
  
  if (!db._pager || !db._header) {
    throw new Error("Single-file database missing pager or header");
  }
  
  const state = getCheckpointState(db);
  
  // If already running, return the existing promise
  if (state.status === 'running') {
    return state.promise;
  }
  
  // If completing, wait for it to finish
  if (state.status === 'completing') {
    // Return a promise that resolves on next tick (completing is synchronous)
    return Promise.resolve();
  }
  
  // Synchronously update in-memory header to switch to secondary region
  // This ensures the next commit will write to secondary region immediately
  const pager = db._pager as FilePager;
  const walBuffer = createWalBuffer(pager, db._header);
  
  db._header = {
    ...db._header,
    activeWalRegion: 1,
    checkpointInProgress: 1,
    walPrimaryHead: walBuffer.getPrimaryHead(),
    walSecondaryHead: walBuffer.getSecondaryHead(),
    changeCounter: db._header.changeCounter + 1n,
  };
  
  // Start the background checkpoint (async - will persist header to disk)
  const promise = backgroundCheckpoint(db);
  db._checkpointState = { status: 'running', promise };
  
  return promise;
}

/**
 * Perform a background checkpoint
 * 
 * Steps:
 * 1. Switch writes to secondary WAL region
 * 2. Set checkpointInProgress flag (for crash recovery)
 * 3. Read primary WAL + snapshot (consistent view)
 * 4. Build new snapshot in memory
 * 5. Write new snapshot to disk
 * 6. Merge secondary into primary, update header, swap mmap
 * 7. Clear checkpointInProgress flag
 */
async function backgroundCheckpoint(db: GraphDB): Promise<void> {
  const pager = db._pager as FilePager;
  let header = db._header!;
  
  const walBuffer = createWalBuffer(pager, header);
  
  // Step 1: Switch writes to secondary region
  walBuffer.switchToSecondary();
  
  // Step 2: Set checkpointInProgress flag and update header
  header = {
    ...header,
    activeWalRegion: 1,
    checkpointInProgress: 1,
    walPrimaryHead: walBuffer.getPrimaryHead(),
    walSecondaryHead: walBuffer.getSecondaryHead(),
    changeCounter: header.changeCounter + 1n,
  };
  await writeHeader(pager, header);
  db._header = header;
  
  try {
    // Step 3-4: Build new snapshot from primary WAL + current snapshot + delta
    // The delta contains all committed changes (including those in primary WAL)
    // We use collectGraphDataForCheckpoint which reads from snapshot + delta
    const { nodes, edges, labels, etypes, propkeys } = collectGraphDataForCheckpoint(db);
    
    // Build new snapshot buffer
    const newGen = header.activeSnapshotGen + 1n;
    const snapshotBuffer = buildSnapshotBuffer({
      generation: newGen,
      nodes,
      edges,
      labels,
      etypes,
      propkeys,
    });
    
    // Step 5: Write new snapshot to file (after WAL area)
    const walEndPage = Number(header.walStartPage + header.walPageCount);
    const newSnapshotStartPage = BigInt(walEndPage);
    const newSnapshotPageCount = BigInt(pagesToStore(snapshotBuffer.length, header.pageSize));
    
    await writeSnapshotPages(pager, Number(newSnapshotStartPage), snapshotBuffer, header.pageSize);
    
    // Step 6: Completing phase - brief lock for final updates
    db._checkpointState = { status: 'completing' };
    
    // Merge secondary records into primary
    // IMPORTANT: Must use CURRENT db._header.walSecondaryHead (not stale local header)
    // because concurrent commits may have written to secondary region while we were
    // building the snapshot. Using stale header would miss those records = DATA LOSS!
    const mergeHeader = {
      ...header,
      walSecondaryHead: db._header!.walSecondaryHead,
    };
    const finalWalBuffer = createWalBuffer(pager, mergeHeader);
    finalWalBuffer.mergeSecondaryIntoPrimary();
    finalWalBuffer.flushPendingWrites();
    
    // Update header with new snapshot and cleared WAL
    const newHeader = updateHeaderForCompaction(
      header,
      newSnapshotStartPage,
      newSnapshotPageCount,
      newGen,
    );
    
    // Also update V2 fields
    const finalHeader = {
      ...newHeader,
      dbSizePages: newSnapshotStartPage + newSnapshotPageCount,
      maxNodeId: BigInt(db._nextNodeId - 1),
      nextTxId: db._nextTxId,
      // Reset V2 fields
      walPrimaryHead: finalWalBuffer.getPrimaryHead(),
      walSecondaryHead: finalWalBuffer.getSecondaryHead(),
      activeWalRegion: 0 as 0 | 1,
      checkpointInProgress: 0 as 0 | 1,
    };
    
    await writeHeader(pager, finalHeader);
    db._header = finalHeader;
    
    // Re-mmap the new snapshot
    if (finalHeader.snapshotPageCount > 0n) {
      const newMmap = pager.mmapRange(
        Number(finalHeader.snapshotStartPage),
        Number(finalHeader.snapshotPageCount)
      );
      
      (db as { _snapshotMmap: Uint8Array | null })._snapshotMmap = newMmap;
      
      // Pre-populate snapshot cache
      if (db._cacheSnapshot !== false) {
        (db as { _snapshotCache: unknown })._snapshotCache = parseSnapshot(newMmap, { skipCrcValidation: true });
      }
    }
    
    // Invalidate old snapshot cache
    invalidateSnapshotCache(db);
    
    // Clear delta
    clearDelta(db._delta);
    
    // Update WAL write position
    db._walWritePos = Number(finalWalBuffer.getPrimaryHead());
    
    // Mark old snapshot pages as free
    if (header.snapshotPageCount > 0n) {
      pager.freePages(Number(header.snapshotStartPage), Number(header.snapshotPageCount));
    }
    
  } catch (error) {
    // On error, try to recover by clearing checkpointInProgress flag
    // This allows normal checkpoint to clean up
    try {
      const recoveryHeader = {
        ...header,
        checkpointInProgress: 0 as 0 | 1,
        activeWalRegion: 0 as 0 | 1,
      };
      await writeHeader(pager, recoveryHeader);
      db._header = recoveryHeader;
    } catch {
      // Ignore recovery errors - crash recovery will handle it
    }
    throw error;
  } finally {
    // Step 7: Mark checkpoint as complete
    db._checkpointState = { status: 'idle' };
  }
}

/**
 * Write snapshot buffer to file pages
 */
async function writeSnapshotPages(
  pager: FilePager,
  startPage: number,
  buffer: Uint8Array,
  pageSize: number,
): Promise<void> {
  const numPages = pagesToStore(buffer.length, pageSize);
  
  // Ensure file is large enough
  const requiredPages = startPage + numPages;
  const currentPages = Math.ceil(pager.fileSize / pageSize);
  
  if (requiredPages > currentPages) {
    pager.allocatePages(requiredPages - currentPages);
  }

  // Write pages
  for (let i = 0; i < numPages; i++) {
    const pageData = new Uint8Array(pageSize);
    const srcOffset = i * pageSize;
    const srcEnd = Math.min(srcOffset + pageSize, buffer.length);
    pageData.set(buffer.subarray(srcOffset, srcEnd));
    
    pager.writePage(startPage + i, pageData);
  }

  // Sync to disk
  await pager.sync();
}

/**
 * Perform blocking checkpoint - merge WAL into snapshot
 * This is the traditional checkpoint approach that blocks writes.
 */
export async function checkpoint(db: GraphDB): Promise<void> {
  if (!db._isSingleFile) {
    throw new Error("checkpoint() only works with single-file databases");
  }
  
  if (!db._pager || !db._header) {
    throw new Error("Single-file database missing pager or header");
  }
  
  // If a background checkpoint is running, wait for it
  const runningPromise = getCheckpointPromise(db);
  if (runningPromise) {
    await runningPromise;
    return;
  }
  
  // Perform compaction (this writes new snapshot and clears WAL)
  await optimizeSingleFile(db);
  
  // Invalidate old snapshot cache
  invalidateSnapshotCache(db);
  
  // Re-mmap the new snapshot area
  const pager = db._pager as FilePager;
  if (db._header.snapshotPageCount > 0n) {
    const newMmap = pager.mmapRange(
      Number(db._header.snapshotStartPage),
      Number(db._header.snapshotPageCount)
    );
    (db as { _snapshotMmap: Uint8Array | null })._snapshotMmap = newMmap;
    
    // Pre-populate snapshot cache immediately after checkpoint
    // This eliminates the parse delay on the first read after checkpoint
    // Skip CRC validation since we just wrote the snapshot
    if (db._cacheSnapshot !== false) {
      (db as { _snapshotCache: unknown })._snapshotCache = parseSnapshot(newMmap, { skipCrcValidation: true });
    }
  }
}
