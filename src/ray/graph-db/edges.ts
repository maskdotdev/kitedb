import { edgePropKey, isNodeDeleted } from "../../core/delta.ts";
import { findEdgeIndex, getPhysNode, getEdgeProp as snapshotGetEdgeProp, getEdgeProps as snapshotGetEdgeProps } from "../../core/snapshot-reader.ts";
import type {
  GraphDB,
  NodeID,
  ETypeID,
  PropKeyID,
  PropValue,
  TxHandle,
  Edge,
} from "../../types.ts";
import { hasEdgeMerged, neighborsIn, neighborsOut } from "../iterators.ts";
import { getCache } from "./cache-helper.ts";

/**
 * Add an edge
 */
export function addEdge(
  handle: TxHandle,
  src: NodeID,
  etype: ETypeID,
  dst: NodeID,
): void {
  const { _db: db, _tx: tx } = handle;

  // Add to pending out-edges
  let outPatches = tx.pendingOutAdd.get(src);
  if (!outPatches) {
    outPatches = [];
    tx.pendingOutAdd.set(src, outPatches);
  }
  outPatches.push({ etype, other: dst });

  // Add to pending in-edges
  let inPatches = tx.pendingInAdd.get(dst);
  if (!inPatches) {
    inPatches = [];
    tx.pendingInAdd.set(dst, inPatches);
  }
  inPatches.push({ etype, other: src });

  // Write-through cache invalidation (immediate)
  const cache = getCache(db);
  if (cache) {
    cache.invalidateEdge(src, etype, dst);
    // Also invalidate traversal caches for both nodes
    cache.invalidateNode(src);
    cache.invalidateNode(dst);
  }
}

/**
 * Delete an edge
 */
export function deleteEdge(
  handle: TxHandle,
  src: NodeID,
  etype: ETypeID,
  dst: NodeID,
): boolean {
  const { _db: db, _tx: tx } = handle;

  // Check if edge exists
  if (!hasEdgeMerged(db._snapshot, db._delta, src, etype, dst)) {
    // Check pending adds
    const outPatches = tx.pendingOutAdd.get(src);
    if (outPatches) {
      const idx = outPatches.findIndex(
        (p) => p.etype === etype && p.other === dst,
      );
      if (idx >= 0) {
        outPatches.splice(idx, 1);

        // Remove from in-adds too
        const inPatches = tx.pendingInAdd.get(dst);
        if (inPatches) {
          const inIdx = inPatches.findIndex(
            (p) => p.etype === etype && p.other === src,
          );
          if (inIdx >= 0) inPatches.splice(inIdx, 1);
        }

        // Write-through cache invalidation
        const cache = getCache(db);
        if (cache) {
          cache.invalidateEdge(src, etype, dst);
          cache.invalidateNode(src);
          cache.invalidateNode(dst);
        }

        return true;
      }
    }
    return false;
  }

  // Add to pending deletions
  let outPatches = tx.pendingOutDel.get(src);
  if (!outPatches) {
    outPatches = [];
    tx.pendingOutDel.set(src, outPatches);
  }
  outPatches.push({ etype, other: dst });

  let inPatches = tx.pendingInDel.get(dst);
  if (!inPatches) {
    inPatches = [];
    tx.pendingInDel.set(dst, inPatches);
  }
  inPatches.push({ etype, other: src });

  // Write-through cache invalidation (immediate)
  const cache = getCache(db);
  if (cache) {
    cache.invalidateEdge(src, etype, dst);
    cache.invalidateNode(src);
    cache.invalidateNode(dst);
  }

  return true;
}

/**
 * Get out-neighbors
 */
export function* getNeighborsOut(
  db: GraphDB,
  nodeId: NodeID,
  etype?: ETypeID,
): Generator<Edge> {
  const cache = getCache(db);
  
  // Check cache first
  if (cache) {
    const cached = cache.getTraversal(nodeId, etype, "out");
    if (cached) {
      for (const edge of cached.neighbors) {
        yield edge;
      }
      // If truncated, fall through to get remaining neighbors
      if (!cached.truncated) {
        return;
      }
    }
  }

  // Collect neighbors
  const neighbors: Edge[] = [];
  for (const edge of neighborsOut(db._snapshot, db._delta, nodeId, etype)) {
    neighbors.push(edge);
    yield edge;
  }

  // Cache the results
  if (cache) {
    cache.setTraversal(nodeId, etype, "out", neighbors);
  }
}

/**
 * Get in-neighbors
 */
export function* getNeighborsIn(
  db: GraphDB,
  nodeId: NodeID,
  etype?: ETypeID,
): Generator<Edge> {
  const cache = getCache(db);
  
  // Check cache first
  if (cache) {
    const cached = cache.getTraversal(nodeId, etype, "in");
    if (cached) {
      for (const edge of cached.neighbors) {
        yield edge;
      }
      // If truncated, fall through to get remaining neighbors
      if (!cached.truncated) {
        return;
      }
    }
  }

  // Collect neighbors
  const neighbors: Edge[] = [];
  for (const edge of neighborsIn(db._snapshot, db._delta, nodeId, etype)) {
    neighbors.push(edge);
    yield edge;
  }

  // Cache the results
  if (cache) {
    cache.setTraversal(nodeId, etype, "in", neighbors);
  }
}

/**
 * Check if edge exists
 */
export function edgeExists(
  db: GraphDB,
  src: NodeID,
  etype: ETypeID,
  dst: NodeID,
): boolean {
  return hasEdgeMerged(db._snapshot, db._delta, src, etype, dst);
}

/**
 * Set an edge property
 */
export function setEdgeProp(
  handle: TxHandle,
  src: NodeID,
  etype: ETypeID,
  dst: NodeID,
  keyId: PropKeyID,
  value: PropValue,
): void {
  const { _db: db, _tx: tx } = handle;
  const edgeKey = `${src}:${etype}:${dst}`;

  let props = tx.pendingEdgeProps.get(edgeKey);
  if (!props) {
    props = new Map();
    tx.pendingEdgeProps.set(edgeKey, props);
  }
  props.set(keyId, value);

  // Write-through cache invalidation (immediate)
  const cache = getCache(db);
  if (cache) {
    cache.invalidateEdge(src, etype, dst);
  }
}

/**
 * Delete an edge property
 */
export function delEdgeProp(
  handle: TxHandle,
  src: NodeID,
  etype: ETypeID,
  dst: NodeID,
  keyId: PropKeyID,
): void {
  const { _db: db, _tx: tx } = handle;
  const edgeKey = `${src}:${etype}:${dst}`;

  let props = tx.pendingEdgeProps.get(edgeKey);
  if (!props) {
    props = new Map();
    tx.pendingEdgeProps.set(edgeKey, props);
  }
  props.set(keyId, null);

  // Write-through cache invalidation (immediate)
  const cache = getCache(db);
  if (cache) {
    cache.invalidateEdge(src, etype, dst);
  }
}

/**
 * Get a specific property for an edge
 * Returns null if the edge doesn't exist or the property is not set
 */
export function getEdgeProp(
  db: GraphDB,
  src: NodeID,
  etype: ETypeID,
  dst: NodeID,
  keyId: PropKeyID,
): PropValue | null {
  // Check cache first
  const cache = getCache(db);
  if (cache) {
    const cached = cache.getEdgeProp(src, etype, dst, keyId);
    if (cached !== undefined) {
      return cached;
    }
  }

  // Check if endpoints are deleted
  if (isNodeDeleted(db._delta, src) || isNodeDeleted(db._delta, dst)) {
    if (cache) {
      cache.setEdgeProp(src, etype, dst, keyId, null);
    }
    return null;
  }

  // Check delta first
  const key = edgePropKey(src, etype, dst);
  const deltaProps = db._delta.edgeProps.get(key);
  if (deltaProps) {
    const deltaValue = deltaProps.get(keyId);
    if (deltaValue !== undefined) {
      if (cache) {
        cache.setEdgeProp(src, etype, dst, keyId, deltaValue);
      }
      return deltaValue; // null means deleted
    }
  }

  // Fall back to snapshot
  if (db._snapshot) {
    const srcPhys = getPhysNode(db._snapshot, src);
    const dstPhys = getPhysNode(db._snapshot, dst);
    if (srcPhys >= 0 && dstPhys >= 0) {
      const edgeIdx = findEdgeIndex(db._snapshot, srcPhys, etype, dstPhys);
      if (edgeIdx >= 0) {
        const value = snapshotGetEdgeProp(db._snapshot, edgeIdx, keyId);
        if (cache) {
          cache.setEdgeProp(src, etype, dst, keyId, value);
        }
        return value;
      }
    }
  }

  // Property not found
  if (cache) {
    cache.setEdgeProp(src, etype, dst, keyId, null);
  }
  return null;
}

/**
 * Get all properties for an edge
 * Returns null if the edge doesn't exist
 */
export function getEdgeProps(
  db: GraphDB,
  src: NodeID,
  etype: ETypeID,
  dst: NodeID,
): Map<PropKeyID, PropValue> | null {
  // Check if endpoints are deleted
  if (isNodeDeleted(db._delta, src) || isNodeDeleted(db._delta, dst)) {
    return null;
  }

  const props = new Map<PropKeyID, PropValue>();
  let edgeExists = false;

  // Get from snapshot first
  if (db._snapshot) {
    const srcPhys = getPhysNode(db._snapshot, src);
    const dstPhys = getPhysNode(db._snapshot, dst);
    if (srcPhys >= 0 && dstPhys >= 0) {
      const edgeIdx = findEdgeIndex(db._snapshot, srcPhys, etype, dstPhys);
      if (edgeIdx >= 0) {
        edgeExists = true;
        const snapshotProps = snapshotGetEdgeProps(db._snapshot, edgeIdx);
        if (snapshotProps) {
          for (const [keyId, value] of snapshotProps) {
            props.set(keyId, value);
          }
        }
      }
    }
  }

  // Apply delta modifications
  const key = edgePropKey(src, etype, dst);
  const deltaProps = db._delta.edgeProps.get(key);
  if (deltaProps) {
    for (const [keyId, value] of deltaProps) {
      if (value === null) {
        props.delete(keyId);
      } else {
        props.set(keyId, value);
      }
    }
  }

  // Check if edge was added in delta
  const addedEdges = db._delta.outAdd.get(src);
  if (addedEdges) {
    for (const patch of addedEdges) {
      if (patch.etype === etype && patch.other === dst) {
        edgeExists = true;
        break;
      }
    }
  }

  if (!edgeExists) {
    return null;
  }

  return props;
}

