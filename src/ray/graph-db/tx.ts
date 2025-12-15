import { join } from "node:path";
import {
  addEdge as deltaAddEdge,
  createNode as deltaCreateNode,
  defineEtype as deltaDefineEtype,
  defineLabel as deltaDefineLabel,
  definePropkey as deltaDefinePropkey,
  deleteEdge as deltaDeleteEdge,
  deleteEdgeProp as deltaDeleteEdgeProp,
  deleteNode as deltaDeleteNode,
  deleteNodeProp as deltaDeleteNodeProp,
  setEdgeProp as deltaSetEdgeProp,
  setNodeProp as deltaSetNodeProp,
} from "../../core/delta.ts";
import {
  appendToWal,
  buildAddEdgePayload,
  buildBeginPayload,
  buildCommitPayload,
  buildCreateNodePayload,
  buildDefineEtypePayload,
  buildDefineLabelPayload,
  buildDefinePropkeyPayload,
  buildDelEdgePropPayload,
  buildDeleteEdgePayload,
  buildDeleteNodePayload,
  buildDelNodePropPayload,
  buildSetEdgePropPayload,
  buildSetNodePropPayload,
  type WalRecord,
} from "../../core/wal.ts";
import type {
  GraphDB,
  NodeID,
  TxHandle,
  TxState,
} from "../../types.ts";
import { WalRecordType } from "../../types.ts";
import { WAL_DIR, walFilename } from "../../constants.ts";
import { getCache } from "./cache-helper.ts";

function createTxState(txid: bigint): TxState {
  return {
    txid,
    pendingCreatedNodes: new Map(),
    pendingDeletedNodes: new Set(),
    pendingOutAdd: new Map(),
    pendingOutDel: new Map(),
    pendingInAdd: new Map(),
    pendingInDel: new Map(),
    pendingNodeProps: new Map(),
    pendingEdgeProps: new Map(),
    pendingNewLabels: new Map(),
    pendingNewEtypes: new Map(),
    pendingNewPropkeys: new Map(),
    pendingKeyUpdates: new Map(),
    pendingKeyDeletes: new Set(),
  };
}

/**
 * Begin a transaction
 */
export function beginTx(db: GraphDB): TxHandle {
  if (db.readOnly) {
    throw new Error("Cannot begin transaction on read-only database");
  }

  if (db._currentTx) {
    throw new Error("Transaction already in progress");
  }

  const txid = db._nextTxId++;
  const tx = createTxState(txid);
  db._currentTx = tx;

  return { _db: db, _tx: tx };
}

/**
 * Commit a transaction
 */
export async function commit(handle: TxHandle): Promise<void> {
  const { _db: db, _tx: tx } = handle;

  if (db._currentTx !== tx) {
    throw new Error("Transaction is not current");
  }

  // Build WAL records
  const records: WalRecord[] = [];

  // BEGIN
  records.push({
    type: WalRecordType.BEGIN,
    txid: tx.txid,
    payload: buildBeginPayload(),
  });

  // Definitions first
  for (const [labelId, name] of tx.pendingNewLabels) {
    records.push({
      type: WalRecordType.DEFINE_LABEL,
      txid: tx.txid,
      payload: buildDefineLabelPayload(labelId, name),
    });
  }

  for (const [etypeId, name] of tx.pendingNewEtypes) {
    records.push({
      type: WalRecordType.DEFINE_ETYPE,
      txid: tx.txid,
      payload: buildDefineEtypePayload(etypeId, name),
    });
  }

  for (const [propkeyId, name] of tx.pendingNewPropkeys) {
    records.push({
      type: WalRecordType.DEFINE_PROPKEY,
      txid: tx.txid,
      payload: buildDefinePropkeyPayload(propkeyId, name),
    });
  }

  // Node creations
  for (const [nodeId, nodeDelta] of tx.pendingCreatedNodes) {
    records.push({
      type: WalRecordType.CREATE_NODE,
      txid: tx.txid,
      payload: buildCreateNodePayload(nodeId, nodeDelta.key),
    });

    // Node properties
    const props = tx.pendingNodeProps.get(nodeId);
    if (props) {
      for (const [keyId, value] of props) {
        if (value !== null) {
          records.push({
            type: WalRecordType.SET_NODE_PROP,
            txid: tx.txid,
            payload: buildSetNodePropPayload(nodeId, keyId, value),
          });
        }
      }
    }
  }

  // Node deletions
  for (const nodeId of tx.pendingDeletedNodes) {
    records.push({
      type: WalRecordType.DELETE_NODE,
      txid: tx.txid,
      payload: buildDeleteNodePayload(nodeId),
    });
  }

  // Edge additions
  for (const [src, patches] of tx.pendingOutAdd) {
    for (const patch of patches) {
      records.push({
        type: WalRecordType.ADD_EDGE,
        txid: tx.txid,
        payload: buildAddEdgePayload(src, patch.etype, patch.other),
      });
    }
  }

  // Edge deletions
  for (const [src, patches] of tx.pendingOutDel) {
    for (const patch of patches) {
      records.push({
        type: WalRecordType.DELETE_EDGE,
        txid: tx.txid,
        payload: buildDeleteEdgePayload(src, patch.etype, patch.other),
      });
    }
  }

  // Existing node property changes
  for (const [nodeId, props] of tx.pendingNodeProps) {
    if (!tx.pendingCreatedNodes.has(nodeId)) {
      for (const [keyId, value] of props) {
        if (value !== null) {
          records.push({
            type: WalRecordType.SET_NODE_PROP,
            txid: tx.txid,
            payload: buildSetNodePropPayload(nodeId, keyId, value),
          });
        } else {
          records.push({
            type: WalRecordType.DEL_NODE_PROP,
            txid: tx.txid,
            payload: buildDelNodePropPayload(nodeId, keyId),
          });
        }
      }
    }
  }

  // Edge property changes
  for (const [edgeKey, props] of tx.pendingEdgeProps) {
    const [srcStr, etypeStr, dstStr] = edgeKey.split(":");
    const src = BigInt(srcStr!);
    const etype = Number.parseInt(etypeStr!, 10);
    const dst = BigInt(dstStr!);

    for (const [keyId, value] of props) {
      if (value !== null) {
        records.push({
          type: WalRecordType.SET_EDGE_PROP,
          txid: tx.txid,
          payload: buildSetEdgePropPayload(src, etype, dst, keyId, value),
        });
      } else {
        records.push({
          type: WalRecordType.DEL_EDGE_PROP,
          txid: tx.txid,
          payload: buildDelEdgePropPayload(src, etype, dst, keyId),
        });
      }
    }
  }

  // COMMIT
  records.push({
    type: WalRecordType.COMMIT,
    txid: tx.txid,
    payload: buildCommitPayload(),
  });

  // Append to WAL
  const walPath = join(
    db.path,
    WAL_DIR,
    walFilename(db._manifest.activeWalSeg),
  );
  db._walOffset = await appendToWal(walPath, records);

  // Apply to delta
  for (const [labelId, name] of tx.pendingNewLabels) {
    deltaDefineLabel(db._delta, labelId, name);
  }

  for (const [etypeId, name] of tx.pendingNewEtypes) {
    deltaDefineEtype(db._delta, etypeId, name);
  }

  for (const [propkeyId, name] of tx.pendingNewPropkeys) {
    deltaDefinePropkey(db._delta, propkeyId, name);
  }

  for (const [nodeId, nodeDelta] of tx.pendingCreatedNodes) {
    deltaCreateNode(db._delta, nodeId, nodeDelta.key);
  }

  for (const nodeId of tx.pendingDeletedNodes) {
    deltaDeleteNode(db._delta, nodeId);
  }

  for (const [src, patches] of tx.pendingOutAdd) {
    for (const patch of patches) {
      deltaAddEdge(db._delta, src, patch.etype, patch.other);
    }
  }

  for (const [src, patches] of tx.pendingOutDel) {
    for (const patch of patches) {
      deltaDeleteEdge(db._delta, src, patch.etype, patch.other);
    }
  }

  for (const [nodeId, props] of tx.pendingNodeProps) {
    const isNew = tx.pendingCreatedNodes.has(nodeId);
    for (const [keyId, value] of props) {
      if (value !== null) {
        deltaSetNodeProp(db._delta, nodeId, keyId, value, isNew);
      } else {
        deltaDeleteNodeProp(db._delta, nodeId, keyId, isNew);
      }
    }
  }

  for (const [edgeKey, props] of tx.pendingEdgeProps) {
    const [srcStr, etypeStr, dstStr] = edgeKey.split(":");
    const src = BigInt(srcStr!);
    const etype = Number.parseInt(etypeStr!, 10);
    const dst = BigInt(dstStr!);

    for (const [keyId, value] of props) {
      if (value !== null) {
        deltaSetEdgeProp(db._delta, src, etype, dst, keyId, value);
      } else {
        deltaDeleteEdgeProp(db._delta, src, etype, dst, keyId);
      }
    }
  }

  // Transaction-aware cache invalidation
  // Invalidate all affected nodes and edges
  const cache = getCache(db);
  if (cache) {
    // Invalidate all nodes that were created, deleted, or modified
    const affectedNodes = new Set<NodeID>();
    for (const nodeId of tx.pendingCreatedNodes.keys()) {
      affectedNodes.add(nodeId);
    }
    for (const nodeId of tx.pendingDeletedNodes) {
      affectedNodes.add(nodeId);
    }
    for (const nodeId of tx.pendingNodeProps.keys()) {
      affectedNodes.add(nodeId);
    }
    for (const nodeId of affectedNodes) {
      cache.invalidateNode(nodeId);
    }

    // Invalidate all edges that were added, deleted, or modified
    const affectedEdges = new Set<string>(); // Format: "src:etype:dst"
    for (const [src, patches] of tx.pendingOutAdd) {
      for (const patch of patches) {
        affectedEdges.add(`${src}:${patch.etype}:${patch.other}`);
      }
    }
    for (const [src, patches] of tx.pendingOutDel) {
      for (const patch of patches) {
        affectedEdges.add(`${src}:${patch.etype}:${patch.other}`);
      }
    }
    for (const edgeKey of tx.pendingEdgeProps.keys()) {
      affectedEdges.add(edgeKey);
    }
    for (const edgeKey of affectedEdges) {
      const [srcStr, etypeStr, dstStr] = edgeKey.split(":");
      const src = BigInt(srcStr!);
      const etype = Number.parseInt(etypeStr!, 10);
      const dst = BigInt(dstStr!);
      cache.invalidateEdge(src, etype, dst);
    }
  }

  db._currentTx = null;
}

/**
 * Rollback a transaction
 */
export function rollback(handle: TxHandle): void {
  const { _db: db, _tx: tx } = handle;

  if (db._currentTx !== tx) {
    throw new Error("Transaction is not current");
  }

  // Simply discard pending state
  db._currentTx = null;
}

