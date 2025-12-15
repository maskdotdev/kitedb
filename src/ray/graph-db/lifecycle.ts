import { join } from "node:path";
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
  snapshotFilename,
  WAL_DIR,
  walFilename,
} from "../../constants.ts";
import { createDelta } from "../../core/delta.ts";
import {
  createEmptyManifest,
  readManifest,
  writeManifest,
} from "../../core/manifest.ts";
import { closeSnapshot, loadSnapshot } from "../../core/snapshot-reader.ts";
import {
  createWalSegment,
  extractCommittedTransactions,
  loadWalSegment,
  parseCreateNodePayload,
  parseDefineEtypePayload,
  parseDefineLabelPayload,
  parseDefinePropkeyPayload,
} from "../../core/wal.ts";
import type {
  GraphDB,
  OpenOptions,
  LockHandle,
} from "../../types.ts";
import { WalRecordType } from "../../types.ts";
import {
  acquireExclusiveLock,
  acquireSharedLock,
  releaseLock,
} from "../../util/lock.ts";
import { CacheManager } from "../../cache/index.ts";
import { replayWalRecord } from "./wal-replay.ts";

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

  // Initialize cache
  const cache = new CacheManager(options.cache);

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
    _cache: cache,
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

