/**
 * Single-file compactor - merges snapshot + delta into new snapshot in-place
 * 
 * The compaction process for single-file format:
 * 1. Build new snapshot data in memory
 * 2. Write new snapshot to free area after current snapshot
 * 3. Update header to point to new snapshot
 * 4. Mark old snapshot pages as free
 * 5. Clear WAL buffer
 */

import { SECTION_ALIGNMENT } from "../constants.ts";
import type {
  DbHeaderV1,
  DeltaState,
  ETypeID,
  GraphDB,
  LabelID,
  NodeID,
  PropKeyID,
  PropValue,
  SnapshotData,
} from "../types.ts";
import { alignUp, readU32At } from "../util/binary.ts";
import type { CompressionOptions } from "../util/compression.ts";
import {
  clearDelta,
  edgePropKey,
  getNodeDelta,
  isEdgeDeleted,
  isNodeDeleted,
} from "./delta.ts";
import { updateHeaderForCompaction, writeHeader } from "./header.ts";
import type { FilePager } from "./pager.ts";
import { pagesToStore } from "./pager.ts";
import {
  findEdgeIndex,
  getEdgeProps,
  getNodeId,
  getNodeProps,
  getOutEdges,
  getString,
  parseSnapshot,
} from "./snapshot-reader.ts";
import {
  type EdgeData,
  type NodeData,
  buildSnapshotBuffer,
} from "./snapshot-writer-buffer.ts";
import { createWalBuffer } from "./wal-buffer.ts";
import { invalidateSnapshotCache } from "../ray/graph-db/snapshot-helper.ts";
import { isCheckpointRunning, getCheckpointPromise } from "../ray/graph-db/checkpoint.ts";

/**
 * Options for single-file optimize operation
 */
export interface SingleFileOptimizeOptions {
  /** Compression options for the new snapshot */
  compression?: CompressionOptions;
}

/**
 * Perform compaction on a single-file database
 * 
 * @param db - The database to compact
 * @param options - Optional settings including compression configuration
 */
export async function optimizeSingleFile(
  db: GraphDB,
  options?: SingleFileOptimizeOptions,
): Promise<void> {
  if (!db._isSingleFile) {
    throw new Error("optimizeSingleFile() is for single-file databases. Use optimize() for multi-file databases.");
  }
  
  if (!db._pager || !db._header) {
    throw new Error("Single-file database missing pager or header");
  }
  
  if (db.readOnly) {
    throw new Error("Cannot compact read-only database");
  }

  if (db._currentTx) {
    throw new Error("Cannot compact with active transaction");
  }

  // Wait for any running background checkpoint to complete
  // This is necessary because:
  // 1. Background checkpoint modifies the snapshot file in place
  // 2. db._snapshotMmap is a live view into the file (via Bun.mmap)
  // 3. If checkpoint writes new section table but we read old mmap size, CRC validation fails
  if (isCheckpointRunning(db)) {
    const checkpointPromise = getCheckpointPromise(db);
    if (checkpointPromise) {
      await checkpointPromise;
    } else {
      // Fallback: poll until checkpoint completes
      while (isCheckpointRunning(db)) {
        await new Promise(resolve => setImmediate(resolve));
      }
    }
  }

  const pager = db._pager as FilePager;
  const header = db._header;

  // Parse current snapshot if exists
  let currentSnapshot: SnapshotData | null = null;
  if (db._snapshotMmap && header.snapshotPageCount > 0n) {
    currentSnapshot = parseSnapshot(db._snapshotMmap);
  }

  // Collect all nodes and edges
  const { nodes, edges, labels, etypes, propkeys } = collectGraphData(
    currentSnapshot,
    db._delta,
  );

  // Build new snapshot buffer
  const newGen = header.activeSnapshotGen + 1n;
  const snapshotBuffer = buildSnapshotBuffer({
    generation: newGen,
    nodes,
    edges,
    labels,
    etypes,
    propkeys,
    compression: options?.compression,
  });

  // Calculate where to place new snapshot
  // Place it after the WAL area
  const walEndPage = Number(header.walStartPage + header.walPageCount);
  const newSnapshotStartPage = BigInt(walEndPage);
  const newSnapshotPageCount = BigInt(pagesToStore(snapshotBuffer.length, header.pageSize));

  // Write snapshot to file
  await writeSnapshotToFile(pager, Number(newSnapshotStartPage), snapshotBuffer, header.pageSize);

  // Update header
  const newHeader = updateHeaderForCompaction(
    header,
    newSnapshotStartPage,
    newSnapshotPageCount,
    newGen,
  );

  // Update database size
  newHeader.dbSizePages = newSnapshotStartPage + newSnapshotPageCount;
  newHeader.maxNodeId = BigInt(db._nextNodeId - 1);
  newHeader.nextTxId = db._nextTxId;

  // Write header atomically
  await writeHeader(pager, newHeader);

  // Update in-memory state
  db._header = newHeader;

  // Re-mmap the new snapshot
  db._snapshotMmap = pager.mmapRange(
    Number(newSnapshotStartPage),
    Number(newSnapshotPageCount),
  );

  // Invalidate snapshot cache so it gets re-parsed from new mmap
  invalidateSnapshotCache(db);

  // Clear WAL
  const walBuffer = createWalBuffer(pager, newHeader);
  walBuffer.clear();
  db._walWritePos = 0;

  // Clear delta
  clearDelta(db._delta);

  // Mark old snapshot pages as free (for future vacuum)
  if (header.snapshotPageCount > 0n) {
    pager.freePages(Number(header.snapshotStartPage), Number(header.snapshotPageCount));
  }
}

/**
 * Write snapshot buffer to file pages
 */
async function writeSnapshotToFile(
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
 * Collect all graph data from snapshot + delta
 */
function collectGraphData(
  snapshot: SnapshotData | null,
  delta: DeltaState,
): {
  nodes: NodeData[];
  edges: EdgeData[];
  labels: Map<LabelID, string>;
  etypes: Map<ETypeID, string>;
  propkeys: Map<PropKeyID, string>;
} {
  const nodes: NodeData[] = [];
  const edges: EdgeData[] = [];
  const labels = new Map<LabelID, string>();
  const etypes = new Map<ETypeID, string>();
  const propkeys = new Map<PropKeyID, string>();

  // First, add data from snapshot
  if (snapshot) {
    const numNodes = Number(snapshot.header.numNodes);

    // Copy labels from snapshot
    for (let i = 1; i <= Number(snapshot.header.numLabels); i++) {
      const stringId = readU32At(snapshot.labelStringIds, i);
      if (stringId > 0) {
        labels.set(i, getString(snapshot, stringId));
      }
    }

    // Copy etypes from snapshot
    for (let i = 1; i <= Number(snapshot.header.numEtypes); i++) {
      const stringId = readU32At(snapshot.etypeStringIds, i);
      if (stringId > 0) {
        etypes.set(i, getString(snapshot, stringId));
      }
    }

    // Copy propkeys from snapshot
    if (snapshot.propkeyStringIds) {
      for (let i = 1; i <= Number(snapshot.header.numPropkeys); i++) {
        const stringId = readU32At(snapshot.propkeyStringIds, i);
        if (stringId > 0) {
          propkeys.set(i, getString(snapshot, stringId));
        }
      }
    }

    // Collect nodes from snapshot
    for (let phys = 0; phys < numNodes; phys++) {
      const nodeId = getNodeId(snapshot, phys);

      // Skip deleted nodes
      if (isNodeDeleted(delta, nodeId)) {
        continue;
      }

      // Get key
      const keyStringId = readU32At(snapshot.nodeKeyString, phys);
      const key = keyStringId > 0 ? getString(snapshot, keyStringId) : undefined;

      // Get properties from snapshot
      const snapshotProps = getNodeProps(snapshot, phys);
      const props = new Map<PropKeyID, PropValue>();

      if (snapshotProps) {
        for (const [keyId, value] of snapshotProps) {
          props.set(keyId, value);
        }
      }

      // Apply delta modifications and collect labels
      const nodeLabels: LabelID[] = [];
      const nodeDelta = getNodeDelta(delta, nodeId);
      if (nodeDelta) {
        if (nodeDelta.props) {
          for (const [keyId, value] of nodeDelta.props) {
            if (value === null) {
              props.delete(keyId);
            } else {
              props.set(keyId, value);
            }
          }
        }
        if (nodeDelta.labels) {
          for (const labelId of nodeDelta.labels) {
            nodeLabels.push(labelId);
          }
        }
      }

      nodes.push({
        nodeId,
        key,
        labels: nodeLabels,
        props,
      });

      // Collect edges from this node
      const outEdges = getOutEdges(snapshot, phys);
      for (const edge of outEdges) {
        const dstNodeId = getNodeId(snapshot, edge.dst);

        // Skip edges to deleted nodes
        if (isNodeDeleted(delta, dstNodeId)) {
          continue;
        }

        // Skip deleted edges
        if (isEdgeDeleted(delta, nodeId, edge.etype, dstNodeId)) {
          continue;
        }

        // Collect edge props from snapshot
        const edgeIdx = findEdgeIndex(snapshot, phys, edge.etype, edge.dst);
        const snapshotEdgeProps = edgeIdx >= 0 ? getEdgeProps(snapshot, edgeIdx) : null;
        const edgeProps = new Map<PropKeyID, PropValue>();

        if (snapshotEdgeProps) {
          for (const [keyId, value] of snapshotEdgeProps) {
            edgeProps.set(keyId, value);
          }
        }

        // Apply delta edge prop modifications
        const deltaEdgeKey = edgePropKey(nodeId, edge.etype, dstNodeId);
        const deltaEdgeProps = delta.edgeProps.get(deltaEdgeKey);
        if (deltaEdgeProps) {
          for (const [keyId, value] of deltaEdgeProps) {
            if (value === null) {
              edgeProps.delete(keyId);
            } else {
              edgeProps.set(keyId, value);
            }
          }
        }

        edges.push({
          src: nodeId,
          etype: edge.etype,
          dst: dstNodeId,
          props: edgeProps,
        });
      }
    }
  }

  // Add new labels from delta
  for (const [labelId, name] of delta.newLabels) {
    labels.set(labelId, name);
  }

  // Add new etypes from delta
  for (const [etypeId, name] of delta.newEtypes) {
    etypes.set(etypeId, name);
  }

  // Add new propkeys from delta
  for (const [propkeyId, name] of delta.newPropkeys) {
    propkeys.set(propkeyId, name);
  }

  // Add nodes created in delta
  for (const [nodeId, nodeDelta] of delta.createdNodes) {
    const props = new Map<PropKeyID, PropValue>();
    if (nodeDelta.props) {
      for (const [keyId, value] of nodeDelta.props) {
        if (value !== null) {
          props.set(keyId, value);
        }
      }
    }

    nodes.push({
      nodeId,
      key: nodeDelta.key,
      labels: nodeDelta.labels ? [...nodeDelta.labels] : [],
      props,
    });
  }

  // Add edges from delta
  for (const [src, patches] of delta.outAdd) {
    for (const patch of patches) {
      // Check if either endpoint is deleted
      if (isNodeDeleted(delta, src) || isNodeDeleted(delta, patch.other)) {
        continue;
      }

      // Collect edge props from delta
      const deltaEdgeKey = edgePropKey(src, patch.etype, patch.other);
      const deltaEdgeProps = delta.edgeProps.get(deltaEdgeKey);
      const edgeProps = new Map<PropKeyID, PropValue>();

      if (deltaEdgeProps) {
        for (const [keyId, value] of deltaEdgeProps) {
          if (value !== null) {
            edgeProps.set(keyId, value);
          }
        }
      }

      edges.push({
        src,
        etype: patch.etype,
        dst: patch.other,
        props: edgeProps,
      });
    }
  }

  return { nodes, edges, labels, etypes, propkeys };
}

/**
 * Options for vacuum operation
 */
export interface VacuumOptions {
  /** 
   * Shrink WAL to minimum size when empty (after compaction).
   * This saves disk space but the WAL will need to grow again on writes.
   * Default: true
   */
  shrinkWal?: boolean;
  
  /**
   * Minimum WAL size to keep (in bytes). Only used when shrinkWal is true.
   * Default: 64KB (16 pages at 4KB page size)
   */
  minWalSize?: number;
}

/** Minimum WAL pages to keep (for small writes before next compaction) */
const MIN_WAL_PAGES = 16n; // 64KB at 4KB page size

/**
 * Vacuum operation - shrink file by reclaiming free pages
 * 
 * This is an expensive operation that:
 * 1. Shrinks WAL area if empty (after compaction)
 * 2. Moves snapshot data to minimize fragmentation
 * 3. Truncates file to remove unused space
 */
export async function vacuumSingleFile(db: GraphDB, options?: VacuumOptions): Promise<void> {
  if (!db._isSingleFile) {
    throw new Error("vacuumSingleFile() is for single-file databases");
  }
  
  if (!db._pager || !db._header) {
    throw new Error("Single-file database missing pager or header");
  }
  
  if (db.readOnly) {
    throw new Error("Cannot vacuum read-only database");
  }

  if (db._currentTx) {
    throw new Error("Cannot vacuum with active transaction");
  }

  const {
    shrinkWal = true,
    minWalSize,
  } = options ?? {};

  const pager = db._pager as FilePager;
  let header = db._header;
  const pageSize = header.pageSize;

  // Calculate minimum WAL pages
  const minWalPages = minWalSize 
    ? BigInt(Math.ceil(minWalSize / pageSize))
    : MIN_WAL_PAGES;

  // Check if WAL can be shrunk (must be empty - head == tail or both 0)
  const walIsEmpty = header.walHead === header.walTail || 
                     (header.walHead === 0n && header.walTail === 0n);
  
  const canShrinkWal = shrinkWal && 
                       walIsEmpty && 
                       header.walPageCount > minWalPages;

  // If no snapshot and no WAL shrinking needed, nothing to do
  if (header.snapshotPageCount === 0n && !canShrinkWal) {
    return;
  }

  // Calculate new WAL size
  const newWalPageCount = canShrinkWal ? minWalPages : header.walPageCount;
  const newWalEndPage = header.walStartPage + newWalPageCount;

  // If we have a snapshot, we need to relocate it right after the (potentially shrunk) WAL
  if (header.snapshotPageCount > 0n) {
    const currentSnapshotStart = header.snapshotStartPage;
    const newSnapshotStart = newWalEndPage;

    // Only relocate if position changes
    if (currentSnapshotStart !== newSnapshotStart) {
      // Read current snapshot data
      const snapshotData = pager.mmapRange(
        Number(currentSnapshotStart),
        Number(header.snapshotPageCount),
      );

      // Write to new location (right after WAL)
      await writeSnapshotToFile(
        pager,
        Number(newSnapshotStart),
        snapshotData,
        pageSize,
      );

      // Update header with new snapshot position
      header = {
        ...header,
        snapshotStartPage: newSnapshotStart,
        changeCounter: header.changeCounter + 1n,
      };
    }

    // Update WAL page count if shrunk
    if (canShrinkWal) {
      header = {
        ...header,
        walPageCount: newWalPageCount,
        changeCounter: header.changeCounter + 1n,
      };
    }

    // Update db size
    header = {
      ...header,
      dbSizePages: header.snapshotStartPage + header.snapshotPageCount,
    };

    // Write updated header
    await writeHeader(pager, header);
    db._header = header;

    // Re-mmap snapshot at new location
    db._snapshotMmap = pager.mmapRange(
      Number(header.snapshotStartPage),
      Number(header.snapshotPageCount),
    );

    // Invalidate snapshot cache so it gets re-parsed from new mmap
    invalidateSnapshotCache(db);

    // Truncate file to new size
    const newFileSize = Number(header.dbSizePages) * pageSize;
    const fs = await import("node:fs");
    fs.ftruncateSync(pager.fd, newFileSize);
  } else if (canShrinkWal) {
    // No snapshot, just shrink WAL
    header = {
      ...header,
      walPageCount: newWalPageCount,
      dbSizePages: 1n + newWalPageCount, // Header + WAL
      changeCounter: header.changeCounter + 1n,
    };

    await writeHeader(pager, header);
    db._header = header;

    // Truncate file
    const newFileSize = Number(header.dbSizePages) * pageSize;
    const fs = await import("node:fs");
    fs.ftruncateSync(pager.fd, newFileSize);
  }
  
  // Reset WAL write position
  db._walWritePos = 0;
}
