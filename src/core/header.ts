/**
 * Database header read/write for single-file format
 * The header occupies the first page (4KB by default)
 */

import {
  DB_HEADER_SIZE,
  DB_HEADER_RESERVED_SIZE,
  DEFAULT_PAGE_SIZE,
  MAGIC_RAYDB,
  MIN_READER_SINGLE_FILE,
  VERSION_SINGLE_FILE,
} from "../constants.ts";
import type { DbHeaderV1 } from "../types.ts";
import {
  readU32,
  readU64,
  viewOf,
  writeU32,
  writeU64,
} from "../util/binary.ts";
import { crc32c } from "../util/crc.ts";
import type { FilePager } from "./pager.ts";

// Header field offsets
const OFFSET_MAGIC = 0;
const OFFSET_PAGE_SIZE = 16;
const OFFSET_VERSION = 20;
const OFFSET_MIN_READER = 24;
const OFFSET_FLAGS = 28;
const OFFSET_CHANGE_COUNTER = 32;
const OFFSET_DB_SIZE_PAGES = 40;
const OFFSET_SNAPSHOT_START = 48;
const OFFSET_SNAPSHOT_COUNT = 56;
const OFFSET_WAL_START = 64;
const OFFSET_WAL_COUNT = 72;
const OFFSET_WAL_HEAD = 80;
const OFFSET_WAL_TAIL = 88;
const OFFSET_ACTIVE_SNAPSHOT_GEN = 96;
const OFFSET_PREV_SNAPSHOT_GEN = 104;
const OFFSET_MAX_NODE_ID = 112;
const OFFSET_NEXT_TX_ID = 120;
const OFFSET_LAST_COMMIT_TS = 128;
const OFFSET_SCHEMA_COOKIE = 136;
// V2 fields for dual-buffer WAL
const OFFSET_WAL_PRIMARY_HEAD = 144;
const OFFSET_WAL_SECONDARY_HEAD = 152;
const OFFSET_ACTIVE_WAL_REGION = 160;
const OFFSET_CHECKPOINT_IN_PROGRESS = 161;
const OFFSET_RESERVED = 162;
const OFFSET_HEADER_CHECKSUM = 176;
// Footer checksum is at page boundary - 4

/**
 * Create a new empty database header
 */
export function createEmptyHeader(
  pageSize: number = DEFAULT_PAGE_SIZE,
  walPageCount: bigint = 16n, // 64KB WAL by default (16 * 4KB pages)
): DbHeaderV1 {
  return {
    magic: new Uint8Array(MAGIC_RAYDB),
    pageSize,
    version: VERSION_SINGLE_FILE,
    minReaderVersion: MIN_READER_SINGLE_FILE,
    flags: 0,
    changeCounter: 0n,
    dbSizePages: 1n + walPageCount, // Header page + WAL pages
    snapshotStartPage: 0n, // No snapshot yet
    snapshotPageCount: 0n,
    walStartPage: 1n, // WAL starts right after header
    walPageCount,
    walHead: 0n,
    walTail: 0n,
    activeSnapshotGen: 0n,
    prevSnapshotGen: 0n,
    maxNodeId: 0n,
    nextTxId: 1n,
    lastCommitTs: 0n,
    schemaCookie: 0n,
    // V2 fields for dual-buffer WAL
    walPrimaryHead: 0n,
    walSecondaryHead: 0n,
    activeWalRegion: 0,
    checkpointInProgress: 0,
    reserved: new Uint8Array(DB_HEADER_RESERVED_SIZE),
    headerChecksum: 0, // Computed on write
    footerChecksum: 0, // Computed on write
  };
}

/**
 * Serialize header to a full page buffer
 */
export function serializeHeader(header: DbHeaderV1): Uint8Array {
  const buffer = new Uint8Array(header.pageSize);
  const view = viewOf(buffer);

  // Magic (16 bytes)
  buffer.set(header.magic, OFFSET_MAGIC);

  // Page size and version info
  writeU32(view, OFFSET_PAGE_SIZE, header.pageSize);
  writeU32(view, OFFSET_VERSION, header.version);
  writeU32(view, OFFSET_MIN_READER, header.minReaderVersion);
  writeU32(view, OFFSET_FLAGS, header.flags);

  // Counters and sizes
  writeU64(view, OFFSET_CHANGE_COUNTER, header.changeCounter);
  writeU64(view, OFFSET_DB_SIZE_PAGES, header.dbSizePages);

  // Snapshot area
  writeU64(view, OFFSET_SNAPSHOT_START, header.snapshotStartPage);
  writeU64(view, OFFSET_SNAPSHOT_COUNT, header.snapshotPageCount);

  // WAL area
  writeU64(view, OFFSET_WAL_START, header.walStartPage);
  writeU64(view, OFFSET_WAL_COUNT, header.walPageCount);
  writeU64(view, OFFSET_WAL_HEAD, header.walHead);
  writeU64(view, OFFSET_WAL_TAIL, header.walTail);

  // Generations
  writeU64(view, OFFSET_ACTIVE_SNAPSHOT_GEN, header.activeSnapshotGen);
  writeU64(view, OFFSET_PREV_SNAPSHOT_GEN, header.prevSnapshotGen);

  // State
  writeU64(view, OFFSET_MAX_NODE_ID, header.maxNodeId);
  writeU64(view, OFFSET_NEXT_TX_ID, header.nextTxId);
  writeU64(view, OFFSET_LAST_COMMIT_TS, header.lastCommitTs);
  writeU64(view, OFFSET_SCHEMA_COOKIE, header.schemaCookie);

  // V2 fields for dual-buffer WAL
  writeU64(view, OFFSET_WAL_PRIMARY_HEAD, header.walPrimaryHead);
  writeU64(view, OFFSET_WAL_SECONDARY_HEAD, header.walSecondaryHead);
  view.setUint8(OFFSET_ACTIVE_WAL_REGION, header.activeWalRegion);
  view.setUint8(OFFSET_CHECKPOINT_IN_PROGRESS, header.checkpointInProgress);

  // Reserved area
  buffer.set(header.reserved, OFFSET_RESERVED);

  // Compute header checksum (bytes 0-175)
  const headerCrc = crc32c(buffer.subarray(0, OFFSET_HEADER_CHECKSUM));
  writeU32(view, OFFSET_HEADER_CHECKSUM, headerCrc);

  // Compute footer checksum (bytes 0 to pageSize-4)
  const footerCrc = crc32c(buffer.subarray(0, header.pageSize - 4));
  writeU32(view, header.pageSize - 4, footerCrc);

  return buffer;
}

/**
 * Parse header from a page buffer
 */
export function parseHeader(buffer: Uint8Array): DbHeaderV1 {
  if (buffer.length < DEFAULT_PAGE_SIZE) {
    throw new Error(`Header buffer too small: ${buffer.length} bytes`);
  }

  const view = viewOf(buffer);

  // Verify magic
  const magic = buffer.subarray(OFFSET_MAGIC, OFFSET_MAGIC + 16);
  for (let i = 0; i < 16; i++) {
    if (magic[i] !== MAGIC_RAYDB[i]) {
      throw new Error(`Invalid database magic at byte ${i}: expected 0x${MAGIC_RAYDB[i]!.toString(16)}, got 0x${magic[i]!.toString(16)}`);
    }
  }

  // Read page size first (needed for footer checksum location)
  const pageSize = readU32(view, OFFSET_PAGE_SIZE);
  
  // Validate page size
  if (pageSize < DEFAULT_PAGE_SIZE || pageSize > 65536 || (pageSize & (pageSize - 1)) !== 0) {
    throw new Error(`Invalid page size: ${pageSize}`);
  }

  // Verify header checksum
  const storedHeaderCrc = readU32(view, OFFSET_HEADER_CHECKSUM);
  const computedHeaderCrc = crc32c(buffer.subarray(0, OFFSET_HEADER_CHECKSUM));
  if (storedHeaderCrc !== computedHeaderCrc) {
    throw new Error(
      `Header checksum mismatch: stored=0x${storedHeaderCrc.toString(16)}, computed=0x${computedHeaderCrc.toString(16)}`
    );
  }

  // Verify footer checksum
  const storedFooterCrc = readU32(view, pageSize - 4);
  const computedFooterCrc = crc32c(buffer.subarray(0, pageSize - 4));
  if (storedFooterCrc !== computedFooterCrc) {
    throw new Error(
      `Footer checksum mismatch: stored=0x${storedFooterCrc.toString(16)}, computed=0x${computedFooterCrc.toString(16)}`
    );
  }

  // Read version info
  const version = readU32(view, OFFSET_VERSION);
  const minReaderVersion = readU32(view, OFFSET_MIN_READER);

  // Check version compatibility
  if (MIN_READER_SINGLE_FILE < minReaderVersion) {
    throw new Error(
      `Database requires reader version ${minReaderVersion}, we are ${MIN_READER_SINGLE_FILE}`
    );
  }

  const flags = readU32(view, OFFSET_FLAGS);

  // Read counters and sizes
  const changeCounter = readU64(view, OFFSET_CHANGE_COUNTER);
  const dbSizePages = readU64(view, OFFSET_DB_SIZE_PAGES);

  // Snapshot area
  const snapshotStartPage = readU64(view, OFFSET_SNAPSHOT_START);
  const snapshotPageCount = readU64(view, OFFSET_SNAPSHOT_COUNT);

  // WAL area
  const walStartPage = readU64(view, OFFSET_WAL_START);
  const walPageCount = readU64(view, OFFSET_WAL_COUNT);
  const walHead = readU64(view, OFFSET_WAL_HEAD);
  const walTail = readU64(view, OFFSET_WAL_TAIL);

  // Generations
  const activeSnapshotGen = readU64(view, OFFSET_ACTIVE_SNAPSHOT_GEN);
  const prevSnapshotGen = readU64(view, OFFSET_PREV_SNAPSHOT_GEN);

  // State
  const maxNodeId = readU64(view, OFFSET_MAX_NODE_ID);
  const nextTxId = readU64(view, OFFSET_NEXT_TX_ID);
  const lastCommitTs = readU64(view, OFFSET_LAST_COMMIT_TS);
  const schemaCookie = readU64(view, OFFSET_SCHEMA_COOKIE);

  // V2 fields for dual-buffer WAL
  const walPrimaryHead = readU64(view, OFFSET_WAL_PRIMARY_HEAD);
  const walSecondaryHead = readU64(view, OFFSET_WAL_SECONDARY_HEAD);
  const activeWalRegion = view.getUint8(OFFSET_ACTIVE_WAL_REGION) as 0 | 1;
  const checkpointInProgress = view.getUint8(OFFSET_CHECKPOINT_IN_PROGRESS) as 0 | 1;

  // Reserved area
  const reserved = new Uint8Array(DB_HEADER_RESERVED_SIZE);
  reserved.set(buffer.subarray(OFFSET_RESERVED, OFFSET_RESERVED + DB_HEADER_RESERVED_SIZE));

  return {
    magic: new Uint8Array(magic),
    pageSize,
    version,
    minReaderVersion,
    flags,
    changeCounter,
    dbSizePages,
    snapshotStartPage,
    snapshotPageCount,
    walStartPage,
    walPageCount,
    walHead,
    walTail,
    activeSnapshotGen,
    prevSnapshotGen,
    maxNodeId,
    nextTxId,
    lastCommitTs,
    schemaCookie,
    walPrimaryHead,
    walSecondaryHead,
    activeWalRegion,
    checkpointInProgress,
    reserved,
    headerChecksum: storedHeaderCrc,
    footerChecksum: storedFooterCrc,
  };
}

/**
 * Read header from a pager
 */
export function readHeader(pager: FilePager): DbHeaderV1 {
  const buffer = pager.readPage(0);
  return parseHeader(buffer);
}

/**
 * Write header to a pager (atomically with sync)
 */
export async function writeHeader(pager: FilePager, header: DbHeaderV1): Promise<void> {
  const buffer = serializeHeader(header);
  pager.writePage(0, buffer);
  await pager.sync();
}

/**
 * Write header synchronously
 */
export function writeHeaderSync(pager: FilePager, header: DbHeaderV1): void {
  const buffer = serializeHeader(header);
  pager.writePage(0, buffer);
  pager.syncSync();
}

/**
 * Update header for a new commit
 */
export function updateHeaderForCommit(
  header: DbHeaderV1,
  walHead: bigint,
  maxNodeId: bigint,
  nextTxId: bigint,
): DbHeaderV1 {
  return {
    ...header,
    changeCounter: header.changeCounter + 1n,
    walHead,
    maxNodeId,
    nextTxId,
    lastCommitTs: BigInt(Date.now()),
  };
}

/**
 * Update header for compaction (new snapshot)
 */
export function updateHeaderForCompaction(
  header: DbHeaderV1,
  snapshotStartPage: bigint,
  snapshotPageCount: bigint,
  newGeneration: bigint,
): DbHeaderV1 {
  return {
    ...header,
    changeCounter: header.changeCounter + 1n,
    snapshotStartPage,
    snapshotPageCount,
    prevSnapshotGen: header.activeSnapshotGen,
    activeSnapshotGen: newGeneration,
    // Reset WAL after compaction (both legacy and V2 fields)
    walHead: 0n,
    walTail: 0n,
    walPrimaryHead: 0n,
    walSecondaryHead: 0n,
    activeWalRegion: 0 as 0 | 1,
    checkpointInProgress: 0 as 0 | 1,
    lastCommitTs: BigInt(Date.now()),
  };
}

/**
 * Check if the database file has a valid header
 */
export function hasValidHeader(buffer: Uint8Array): boolean {
  if (buffer.length < 20) {
    return false;
  }

  // Check magic
  for (let i = 0; i < 16; i++) {
    if (buffer[i] !== MAGIC_RAYDB[i]) {
      return false;
    }
  }

  return true;
}

/**
 * Calculate WAL area size in bytes
 */
export function getWalAreaSize(header: DbHeaderV1): number {
  return Number(header.walPageCount) * header.pageSize;
}

/**
 * Calculate snapshot area offset in bytes
 */
export function getSnapshotAreaOffset(header: DbHeaderV1): number {
  return Number(header.snapshotStartPage) * header.pageSize;
}

/**
 * Calculate WAL area offset in bytes
 */
export function getWalAreaOffset(header: DbHeaderV1): number {
  return Number(header.walStartPage) * header.pageSize;
}
