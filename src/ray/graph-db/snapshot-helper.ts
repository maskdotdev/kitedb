/**
 * Snapshot access helper - unified access for both formats
 * 
 * For multi-file: returns db._snapshot directly
 * For single-file: parses _snapshotMmap and optionally caches result
 */

import type {
  DeltaState,
  ETypeID,
  GraphDB,
  LabelID,
  NodeID,
  PropKeyID,
  PropValue,
  SnapshotData,
} from "../../types.ts";
import { parseSnapshot } from "../../core/snapshot-reader.ts";
import {
  edgePropKey,
  getNodeDelta,
  isEdgeDeleted,
  isNodeDeleted,
} from "../../core/delta.ts";
import {
  findEdgeIndex,
  getEdgeProps,
  getNodeId,
  getNodeProps,
  getOutEdges,
  getString,
} from "../../core/snapshot-reader.ts";
import { readU32At } from "../../util/binary.ts";
import type { NodeData, EdgeData } from "../../core/snapshot-writer-buffer.ts";

/**
 * Get the snapshot data for a database
 * Works with both multi-file and single-file formats
 */
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
  
  // Parse snapshot - skip CRC validation since we trust mmap'd data
  // CRC was validated when the snapshot was first loaded or written
  const snapshot = parseSnapshot(db._snapshotMmap, { skipCrcValidation: true });
  
  // Cache if enabled (default: true)
  if (db._cacheSnapshot !== false) {
    (db as { _snapshotCache: SnapshotData | null })._snapshotCache = snapshot;
  }
  
  return snapshot;
}

/**
 * Invalidate the cached snapshot (call after checkpoint)
 */
export function invalidateSnapshotCache(db: GraphDB): void {
  if (db._isSingleFile) {
    (db as { _snapshotCache: SnapshotData | null })._snapshotCache = null;
  }
}

/**
 * Collect all graph data from snapshot + delta for checkpoint
 * This is used by background checkpoint to build the new snapshot.
 */
export function collectGraphDataForCheckpoint(db: GraphDB): {
  nodes: NodeData[];
  edges: EdgeData[];
  labels: Map<LabelID, string>;
  etypes: Map<ETypeID, string>;
  propkeys: Map<PropKeyID, string>;
} {
  const snapshot = getSnapshot(db);
  const delta = db._delta;
  
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
