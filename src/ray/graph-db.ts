/**
 * Main GraphDB handle with transaction logic
 */

import { join } from "node:path";
import { checkSnapshot as checkSnapshotFn } from "../check/checker.ts";
import {
	COMPACT_EDGE_RATIO,
	COMPACT_NODE_RATIO,
	COMPACT_WAL_SIZE,
	INITIAL_ETYPE_ID,
	INITIAL_LABEL_ID,
	INITIAL_NODE_ID,
	INITIAL_PROPKEY_ID,
	INITIAL_TX_ID,
	INITIAL_WAL_SEG,
	MANIFEST_FILENAME,
	SNAPSHOTS_DIR,
	WAL_DIR,
	snapshotFilename,
	walFilename,
} from "../constants.ts";
import {
	clearDelta,
	createDelta,
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
	edgePropKey,
	getDeltaStats,
	getNodeDelta,
	isNodeCreated,
	isNodeDeleted,
} from "../core/delta.ts";
import {
	createEmptyManifest,
	readManifest,
	writeManifest,
} from "../core/manifest.ts";
import {
	closeSnapshot,
	findEdgeIndex,
	getPhysNode,
	loadSnapshot,
	getEdgeProp as snapshotGetEdgeProp,
	getEdgeProps as snapshotGetEdgeProps,
	getNodeProp as snapshotGetNodeProp,
	getNodeProps as snapshotGetNodeProps,
} from "../core/snapshot-reader.ts";
import {
	type ParsedWalRecord,
	type WalRecord,
	appendToWal,
	buildAddEdgePayload,
	buildBeginPayload,
	buildCommitPayload,
	buildCreateNodePayload,
	buildDefineEtypePayload,
	buildDefineLabelPayload,
	buildDefinePropkeyPayload,
	buildDelEdgePropPayload,
	buildDelNodePropPayload,
	buildDeleteEdgePayload,
	buildDeleteNodePayload,
	buildSetEdgePropPayload,
	buildSetNodePropPayload,
	buildWalRecord,
	createWalSegment,
	extractCommittedTransactions,
	loadWalSegment,
	parseAddEdgePayload,
	parseCreateNodePayload,
	parseDefineEtypePayload,
	parseDefineLabelPayload,
	parseDefinePropkeyPayload,
	parseDelEdgePropPayload,
	parseDelNodePropPayload,
	parseDeleteEdgePayload,
	parseDeleteNodePayload,
	parseSetEdgePropPayload,
	parseSetNodePropPayload,
} from "../core/wal.ts";
import type {
	CheckResult,
	DbStats,
	DeltaState,
	ETypeID,
	Edge,
	GraphDB,
	LabelID,
	NodeID,
	NodeOpts,
	OpenOptions,
	PropKeyID,
	PropValue,
	TxHandle,
	TxState,
} from "../types.ts";
import { WalRecordType } from "../types.ts";
import {
	type LockHandle,
	acquireExclusiveLock,
	acquireSharedLock,
	releaseLock,
} from "../util/lock.ts";
import { hasEdgeMerged, neighborsIn, neighborsOut } from "./iterators.ts";
import { lookupByKey } from "./key-index.ts";

// ============================================================================
// Database lifecycle
// ============================================================================

/**
 * Open a graph database
 */
export async function openGraphDB(
	path: string,
	options: OpenOptions = {},
): Promise<GraphDB> {
	const { readOnly = false, createIfMissing = true, lockFile = true } = options;

	// Ensure directory exists
	const fs = await import("node:fs/promises");

	const manifestPath = join(path, MANIFEST_FILENAME);
	let manifestExists = false;

	try {
		await fs.access(manifestPath);
		manifestExists = true;
	} catch {
		manifestExists = false;
	}

	if (!manifestExists && !createIfMissing) {
		throw new Error(`Database does not exist at ${path}`);
	}

	// Create directory structure
	if (!manifestExists) {
		await fs.mkdir(path, { recursive: true });
		await fs.mkdir(join(path, SNAPSHOTS_DIR), { recursive: true });
		await fs.mkdir(join(path, WAL_DIR), { recursive: true });
	}

	// Acquire lock
	let lockFd: LockHandle | null = null;
	if (lockFile) {
		if (readOnly) {
			lockFd = await acquireSharedLock(path);
		} else {
			lockFd = await acquireExclusiveLock(path);
			if (!lockFd) {
				throw new Error(
					"Failed to acquire exclusive lock - database may be in use",
				);
			}
		}
	}

	// Read or create manifest
	let manifest = await readManifest(path);
	if (!manifest) {
		if (readOnly) {
			throw new Error("Cannot create database in read-only mode");
		}
		manifest = createEmptyManifest();
		await writeManifest(path, manifest);
	}

	// Load snapshot if exists
	let snapshot = null;
	if (manifest.activeSnapshotGen > 0n) {
		try {
			snapshot = await loadSnapshot(path, manifest.activeSnapshotGen);
		} catch (err) {
			console.warn(`Failed to load snapshot: ${err}`);
		}
	}

	// Initialize delta
	const delta = createDelta();

	// Initialize ID allocators
	let nextNodeId = INITIAL_NODE_ID;
	let nextLabelId = INITIAL_LABEL_ID;
	let nextEtypeId = INITIAL_ETYPE_ID;
	let nextPropkeyId = INITIAL_PROPKEY_ID;

	if (snapshot) {
		nextNodeId = snapshot.header.maxNodeId + 1n;
		nextLabelId = Number(snapshot.header.numLabels) + 1;
		nextEtypeId = Number(snapshot.header.numEtypes) + 1;
		nextPropkeyId = Number(snapshot.header.numPropkeys) + 1;
	}

	// Ensure WAL exists
	let walOffset = 0;
	const walPath = join(path, WAL_DIR, walFilename(manifest.activeWalSeg));

	try {
		const walFile = Bun.file(walPath);
		if (!(await walFile.exists())) {
			if (!readOnly) {
				await createWalSegment(path, manifest.activeWalSeg);
			}
		}
		walOffset = (await walFile.arrayBuffer()).byteLength;
	} catch {
		if (!readOnly) {
			await createWalSegment(path, manifest.activeWalSeg);
			const walFile = Bun.file(walPath);
			walOffset = (await walFile.arrayBuffer()).byteLength;
		}
	}

	// Replay WAL for recovery
	const walData = await loadWalSegment(path, manifest.activeWalSeg);
	let nextTxId = INITIAL_TX_ID;

	if (walData) {
		const committed = extractCommittedTransactions(walData.records);

		for (const [txid, records] of committed) {
			if (txid >= nextTxId) {
				nextTxId = txid + 1n;
			}

			// Replay each record
			for (const record of records) {
				replayWalRecord(record, delta);

				// Update ID allocators
				if (record.type === WalRecordType.CREATE_NODE) {
					const data = parseCreateNodePayload(record.payload);
					if (data.nodeId >= nextNodeId) {
						nextNodeId = data.nodeId + 1n;
					}
				} else if (record.type === WalRecordType.DEFINE_LABEL) {
					const data = parseDefineLabelPayload(record.payload);
					if (data.labelId >= nextLabelId) {
						nextLabelId = data.labelId + 1;
					}
				} else if (record.type === WalRecordType.DEFINE_ETYPE) {
					const data = parseDefineEtypePayload(record.payload);
					if (data.etypeId >= nextEtypeId) {
						nextEtypeId = data.etypeId + 1;
					}
				} else if (record.type === WalRecordType.DEFINE_PROPKEY) {
					const data = parseDefinePropkeyPayload(record.payload);
					if (data.propkeyId >= nextPropkeyId) {
						nextPropkeyId = data.propkeyId + 1;
					}
				}
			}
		}

		walOffset =
			walData.records.length > 0
				? walData.records[walData.records.length - 1]!.recordEnd
				: walOffset;
	}

	return {
		path,
		readOnly,
		_manifest: manifest,
		_snapshot: snapshot,
		_delta: delta,
		_walFd: null,
		_walOffset: walOffset,
		_nextNodeId: nextNodeId,
		_nextLabelId: nextLabelId,
		_nextEtypeId: nextEtypeId,
		_nextPropkeyId: nextPropkeyId,
		_nextTxId: nextTxId,
		_currentTx: null,
		_lockFd: lockFd,
	};
}

/**
 * Close the database
 */
export async function closeGraphDB(db: GraphDB): Promise<void> {
	// Close snapshot
	if (db._snapshot) {
		closeSnapshot(db._snapshot);
		db._snapshot = null;
	}

	// Release lock
	if (db._lockFd) {
		releaseLock(db._lockFd as LockHandle);
		db._lockFd = null;
	}
}

// ============================================================================
// WAL replay helper
// ============================================================================

function replayWalRecord(record: ParsedWalRecord, delta: DeltaState): void {
	switch (record.type) {
		case WalRecordType.CREATE_NODE: {
			const data = parseCreateNodePayload(record.payload);
			deltaCreateNode(delta, data.nodeId, data.key);
			break;
		}
		case WalRecordType.DELETE_NODE: {
			const data = parseDeleteNodePayload(record.payload);
			deltaDeleteNode(delta, data.nodeId);
			break;
		}
		case WalRecordType.ADD_EDGE: {
			const data = parseAddEdgePayload(record.payload);
			deltaAddEdge(delta, data.src, data.etype, data.dst);
			break;
		}
		case WalRecordType.DELETE_EDGE: {
			const data = parseDeleteEdgePayload(record.payload);
			deltaDeleteEdge(delta, data.src, data.etype, data.dst);
			break;
		}
		case WalRecordType.DEFINE_LABEL: {
			const data = parseDefineLabelPayload(record.payload);
			deltaDefineLabel(delta, data.labelId, data.name);
			break;
		}
		case WalRecordType.DEFINE_ETYPE: {
			const data = parseDefineEtypePayload(record.payload);
			deltaDefineEtype(delta, data.etypeId, data.name);
			break;
		}
		case WalRecordType.DEFINE_PROPKEY: {
			const data = parseDefinePropkeyPayload(record.payload);
			deltaDefinePropkey(delta, data.propkeyId, data.name);
			break;
		}
		case WalRecordType.SET_NODE_PROP: {
			const data = parseSetNodePropPayload(record.payload);
			const isNew = isNodeCreated(delta, data.nodeId);
			deltaSetNodeProp(delta, data.nodeId, data.keyId, data.value, isNew);
			break;
		}
		case WalRecordType.DEL_NODE_PROP: {
			const data = parseDelNodePropPayload(record.payload);
			const isNew = isNodeCreated(delta, data.nodeId);
			deltaDeleteNodeProp(delta, data.nodeId, data.keyId, isNew);
			break;
		}
		case WalRecordType.SET_EDGE_PROP: {
			const data = parseSetEdgePropPayload(record.payload);
			deltaSetEdgeProp(
				delta,
				data.src,
				data.etype,
				data.dst,
				data.keyId,
				data.value,
			);
			break;
		}
		case WalRecordType.DEL_EDGE_PROP: {
			const data = parseDelEdgePropPayload(record.payload);
			deltaDeleteEdgeProp(delta, data.src, data.etype, data.dst, data.keyId);
			break;
		}
	}
}

// ============================================================================
// Transactions
// ============================================================================

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

// ============================================================================
// Node operations
// ============================================================================

/**
 * Create a new node
 */
export function createNode(handle: TxHandle, opts: NodeOpts = {}): NodeID {
	const { _db: db, _tx: tx } = handle;

	const nodeId = db._nextNodeId++;

	const nodeDelta = {
		key: opts.key,
		labels: new Set(opts.labels ?? []),
		labelsDeleted: new Set<LabelID>(),
		props: new Map<PropKeyID, PropValue | null>(),
	};

	tx.pendingCreatedNodes.set(nodeId, nodeDelta);

	if (opts.key) {
		tx.pendingKeyUpdates.set(opts.key, nodeId);
	}

	if (opts.props) {
		tx.pendingNodeProps.set(
			nodeId,
			new Map([...opts.props].map(([k, v]) => [k, v])),
		);
	}

	return nodeId;
}

/**
 * Delete a node
 */
export function deleteNode(handle: TxHandle, nodeId: NodeID): boolean {
	const { _db: db, _tx: tx } = handle;

	// Check if it was created in this transaction
	if (tx.pendingCreatedNodes.has(nodeId)) {
		const nodeDelta = tx.pendingCreatedNodes.get(nodeId)!;
		if (nodeDelta.key) {
			tx.pendingKeyUpdates.delete(nodeDelta.key);
		}
		tx.pendingCreatedNodes.delete(nodeId);
		tx.pendingNodeProps.delete(nodeId);
		tx.pendingOutAdd.delete(nodeId);
		tx.pendingOutDel.delete(nodeId);
		tx.pendingInAdd.delete(nodeId);
		tx.pendingInDel.delete(nodeId);
		return true;
	}

	// Check if node exists
	const existsInSnapshot =
		db._snapshot && getPhysNode(db._snapshot, nodeId) >= 0;
	const existsInDelta = isNodeCreated(db._delta, nodeId);

	if (!existsInSnapshot && !existsInDelta) {
		return false;
	}

	if (isNodeDeleted(db._delta, nodeId)) {
		return false;
	}

	tx.pendingDeletedNodes.add(nodeId);
	return true;
}

/**
 * Get a node by key
 */
export function getNodeByKey(db: GraphDB, key: string): NodeID | null {
	// Check current transaction first
	if (db._currentTx) {
		const pending = db._currentTx.pendingKeyUpdates.get(key);
		if (pending !== undefined) return pending;
		if (db._currentTx.pendingKeyDeletes.has(key)) return null;
	}

	return lookupByKey(db._snapshot, db._delta, key);
}

/**
 * Check if a node exists
 */
export function nodeExists(db: GraphDB, nodeId: NodeID): boolean {
	// Check current transaction
	if (db._currentTx) {
		if (db._currentTx.pendingCreatedNodes.has(nodeId)) return true;
		if (db._currentTx.pendingDeletedNodes.has(nodeId)) return false;
	}

	// Check delta
	if (isNodeDeleted(db._delta, nodeId)) return false;
	if (isNodeCreated(db._delta, nodeId)) return true;

	// Check snapshot
	if (db._snapshot) {
		return getPhysNode(db._snapshot, nodeId) >= 0;
	}

	return false;
}

// ============================================================================
// Edge operations
// ============================================================================

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
	yield* neighborsOut(db._snapshot, db._delta, nodeId, etype);
}

/**
 * Get in-neighbors
 */
export function* getNeighborsIn(
	db: GraphDB,
	nodeId: NodeID,
	etype?: ETypeID,
): Generator<Edge> {
	yield* neighborsIn(db._snapshot, db._delta, nodeId, etype);
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

// ============================================================================
// Property operations
// ============================================================================

/**
 * Set a node property
 */
export function setNodeProp(
	handle: TxHandle,
	nodeId: NodeID,
	keyId: PropKeyID,
	value: PropValue,
): void {
	const { _tx: tx } = handle;

	let props = tx.pendingNodeProps.get(nodeId);
	if (!props) {
		props = new Map();
		tx.pendingNodeProps.set(nodeId, props);
	}
	props.set(keyId, value);
}

/**
 * Delete a node property
 */
export function delNodeProp(
	handle: TxHandle,
	nodeId: NodeID,
	keyId: PropKeyID,
): void {
	const { _tx: tx } = handle;

	let props = tx.pendingNodeProps.get(nodeId);
	if (!props) {
		props = new Map();
		tx.pendingNodeProps.set(nodeId, props);
	}
	props.set(keyId, null);
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
	const { _tx: tx } = handle;
	const edgeKey = `${src}:${etype}:${dst}`;

	let props = tx.pendingEdgeProps.get(edgeKey);
	if (!props) {
		props = new Map();
		tx.pendingEdgeProps.set(edgeKey, props);
	}
	props.set(keyId, value);
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
	const { _tx: tx } = handle;
	const edgeKey = `${src}:${etype}:${dst}`;

	let props = tx.pendingEdgeProps.get(edgeKey);
	if (!props) {
		props = new Map();
		tx.pendingEdgeProps.set(edgeKey, props);
	}
	props.set(keyId, null);
}

/**
 * Get a specific property for a node
 * Returns null if the node doesn't exist, is deleted, or the property is not set
 */
export function getNodeProp(
	db: GraphDB,
	nodeId: NodeID,
	keyId: PropKeyID,
): PropValue | null {
	// Check if node is deleted
	if (isNodeDeleted(db._delta, nodeId)) {
		return null;
	}

	// Check delta first (modifications take precedence)
	const nodeDelta = getNodeDelta(db._delta, nodeId);
	if (nodeDelta) {
		const deltaValue = nodeDelta.props.get(keyId);
		if (deltaValue !== undefined) {
			return deltaValue; // null means deleted
		}
	}

	// Fall back to snapshot
	if (db._snapshot) {
		const phys = getPhysNode(db._snapshot, nodeId);
		if (phys >= 0) {
			return snapshotGetNodeProp(db._snapshot, phys, keyId);
		}
	}

	return null;
}

/**
 * Get all properties for a node
 * Returns null if the node doesn't exist or is deleted
 */
export function getNodeProps(
	db: GraphDB,
	nodeId: NodeID,
): Map<PropKeyID, PropValue> | null {
	// Check if node is deleted
	if (isNodeDeleted(db._delta, nodeId)) {
		return null;
	}

	const props = new Map<PropKeyID, PropValue>();

	// Get from snapshot first
	if (db._snapshot) {
		const phys = getPhysNode(db._snapshot, nodeId);
		if (phys >= 0) {
			const snapshotProps = snapshotGetNodeProps(db._snapshot, phys);
			if (snapshotProps) {
				for (const [keyId, value] of snapshotProps) {
					props.set(keyId, value);
				}
			}
		}
	}

	// Apply delta modifications
	const nodeDelta = getNodeDelta(db._delta, nodeId);
	if (nodeDelta) {
		for (const [keyId, value] of nodeDelta.props) {
			if (value === null) {
				props.delete(keyId);
			} else {
				props.set(keyId, value);
			}
		}
	}

	// Check if node exists at all
	if (!nodeDelta && db._snapshot) {
		const phys = getPhysNode(db._snapshot, nodeId);
		if (phys < 0) {
			return null;
		}
	} else if (!nodeDelta && !db._snapshot) {
		return null;
	}

	return props;
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
	// Check if endpoints are deleted
	if (isNodeDeleted(db._delta, src) || isNodeDeleted(db._delta, dst)) {
		return null;
	}

	// Check delta first
	const key = edgePropKey(src, etype, dst);
	const deltaProps = db._delta.edgeProps.get(key);
	if (deltaProps) {
		const deltaValue = deltaProps.get(keyId);
		if (deltaValue !== undefined) {
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
				return snapshotGetEdgeProp(db._snapshot, edgeIdx, keyId);
			}
		}
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

// ============================================================================
// Definition operations
// ============================================================================

/**
 * Define a new label
 */
export function defineLabel(handle: TxHandle, name: string): LabelID {
	const { _db: db, _tx: tx } = handle;
	const labelId = db._nextLabelId++;
	tx.pendingNewLabels.set(labelId, name);
	return labelId;
}

/**
 * Define a new edge type
 */
export function defineEtype(handle: TxHandle, name: string): ETypeID {
	const { _db: db, _tx: tx } = handle;
	const etypeId = db._nextEtypeId++;
	tx.pendingNewEtypes.set(etypeId, name);
	return etypeId;
}

/**
 * Define a new property key
 */
export function definePropkey(handle: TxHandle, name: string): PropKeyID {
	const { _db: db, _tx: tx } = handle;
	const propkeyId = db._nextPropkeyId++;
	tx.pendingNewPropkeys.set(propkeyId, name);
	return propkeyId;
}

// ============================================================================
// Stats and maintenance
// ============================================================================

/**
 * Get database statistics
 */
export function stats(db: GraphDB): DbStats {
	const snapshotNodes = db._snapshot ? db._snapshot.header.numNodes : 0n;
	const snapshotEdges = db._snapshot ? db._snapshot.header.numEdges : 0n;
	const snapshotMaxNodeId = db._snapshot ? db._snapshot.header.maxNodeId : 0n;

	const deltaStats = getDeltaStats(db._delta);

	const recommendCompact =
		BigInt(deltaStats.edgesAdded + deltaStats.edgesDeleted) >
			snapshotEdges / 10n ||
		BigInt(deltaStats.nodesCreated + deltaStats.nodesDeleted) >
			snapshotNodes / 10n ||
		db._walOffset > COMPACT_WAL_SIZE;

	return {
		snapshotGen: db._manifest.activeSnapshotGen,
		snapshotNodes,
		snapshotEdges,
		snapshotMaxNodeId,
		deltaNodesCreated: BigInt(deltaStats.nodesCreated),
		deltaNodesDeleted: BigInt(deltaStats.nodesDeleted),
		deltaEdgesAdded: BigInt(deltaStats.edgesAdded),
		deltaEdgesDeleted: BigInt(deltaStats.edgesDeleted),
		walSegment: db._manifest.activeWalSeg,
		walBytes: BigInt(db._walOffset),
		recommendCompact,
	};
}

/**
 * Check database integrity
 */
export function check(db: GraphDB): CheckResult {
	if (!db._snapshot) {
		return { valid: true, errors: [], warnings: ["No snapshot to check"] };
	}

	return checkSnapshotFn(db._snapshot);
}
