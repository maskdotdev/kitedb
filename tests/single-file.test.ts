/**
 * Tests for single-file database format (.raydb)
 */

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  createEmptyHeader,
  parseHeader,
  serializeHeader,
  hasValidHeader,
  updateHeaderForCommit,
  updateHeaderForCompaction,
  getWalAreaOffset,
  getWalAreaSize,
  getSnapshotAreaOffset,
} from "../src/core/header.ts";
import { FilePager, createPager, openPager, isValidPageSize, pagesToStore } from "../src/core/pager.ts";
import { WalBuffer, createWalBuffer, calculateWalSize, walSizeToPageCount } from "../src/core/wal-buffer.ts";
import { openSingleFileDB, closeSingleFileDB, isSingleFilePath } from "../src/ray/graph-db/single-file.ts";
import {
  DEFAULT_PAGE_SIZE,
  MAGIC_RAYDB,
  VERSION_SINGLE_FILE,
  WAL_DEFAULT_SIZE,
  LOCK_BYTE_OFFSET,
  LOCK_BYTE_RANGE,
} from "../src/constants.ts";
import { WalBufferFullError, WalRecordType } from "../src/types.ts";
import { buildWalRecord } from "../src/core/wal.ts";

describe("Single-File Format Constants", () => {
  test("magic bytes are correct", () => {
    const expected = new TextEncoder().encode("RayDB format 1\0\0");
    expect(MAGIC_RAYDB).toEqual(expected);
  });

  test("default page size is 4KB", () => {
    expect(DEFAULT_PAGE_SIZE).toBe(4096);
  });

  test("lock byte offset is 2^30", () => {
    expect(LOCK_BYTE_OFFSET).toBe(0x40000000);
    expect(LOCK_BYTE_OFFSET).toBe(1073741824); // 1GB
  });

  test("lock byte range is 512 bytes", () => {
    expect(LOCK_BYTE_RANGE).toBe(512);
  });
});

describe("Page Size Validation", () => {
  test("validates minimum page size", () => {
    expect(isValidPageSize(2048)).toBe(false);
    expect(isValidPageSize(4096)).toBe(true);
  });

  test("validates maximum page size", () => {
    expect(isValidPageSize(65536)).toBe(true);
    expect(isValidPageSize(131072)).toBe(false);
  });

  test("validates power of 2", () => {
    expect(isValidPageSize(4096)).toBe(true);
    expect(isValidPageSize(8192)).toBe(true);
    expect(isValidPageSize(5000)).toBe(false);
    expect(isValidPageSize(6144)).toBe(false);
  });
});

describe("Database Header", () => {
  test("creates empty header with defaults", () => {
    const header = createEmptyHeader();
    
    expect(header.pageSize).toBe(DEFAULT_PAGE_SIZE);
    expect(header.version).toBe(VERSION_SINGLE_FILE);
    expect(header.minReaderVersion).toBe(1);
    expect(header.flags).toBe(0);
    expect(header.changeCounter).toBe(0n);
    expect(header.activeSnapshotGen).toBe(0n);
    expect(header.prevSnapshotGen).toBe(0n);
    expect(header.maxNodeId).toBe(0n);
    expect(header.nextTxId).toBe(1n);
    expect(header.walStartPage).toBe(1n);
  });

  test("creates empty header with custom page size", () => {
    const header = createEmptyHeader(8192);
    expect(header.pageSize).toBe(8192);
  });

  test("creates empty header with custom WAL size", () => {
    const header = createEmptyHeader(4096, 32n);
    expect(header.walPageCount).toBe(32n);
    expect(header.dbSizePages).toBe(33n); // 1 header + 32 WAL pages
  });

  test("serializes and parses header correctly", () => {
    const original = createEmptyHeader();
    original.changeCounter = 42n;
    original.maxNodeId = 1000n;
    original.nextTxId = 50n;
    original.activeSnapshotGen = 5n;
    original.prevSnapshotGen = 4n;
    original.schemaCookie = 123456789n;
    
    const buffer = serializeHeader(original);
    expect(buffer.length).toBe(DEFAULT_PAGE_SIZE);
    
    const parsed = parseHeader(buffer);
    
    expect(parsed.pageSize).toBe(original.pageSize);
    expect(parsed.version).toBe(original.version);
    expect(parsed.changeCounter).toBe(original.changeCounter);
    expect(parsed.maxNodeId).toBe(original.maxNodeId);
    expect(parsed.nextTxId).toBe(original.nextTxId);
    expect(parsed.activeSnapshotGen).toBe(original.activeSnapshotGen);
    expect(parsed.prevSnapshotGen).toBe(original.prevSnapshotGen);
    expect(parsed.schemaCookie).toBe(original.schemaCookie);
  });

  test("validates header magic", () => {
    const buffer = serializeHeader(createEmptyHeader());
    expect(hasValidHeader(buffer)).toBe(true);
    
    const invalidBuffer = new Uint8Array(4096);
    invalidBuffer.set([0, 0, 0, 0], 0);
    expect(hasValidHeader(invalidBuffer)).toBe(false);
  });

  test("detects header checksum corruption", () => {
    const buffer = serializeHeader(createEmptyHeader());
    
    // Corrupt a byte in the header
    buffer[50] = buffer[50]! ^ 0xFF;
    
    expect(() => parseHeader(buffer)).toThrow(/checksum/i);
  });

  test("updates header for commit", () => {
    const original = createEmptyHeader();
    const updated = updateHeaderForCommit(
      original,
      1024n, // walHead
      100n,  // maxNodeId
      10n,   // nextTxId
    );
    
    expect(updated.changeCounter).toBe(1n);
    expect(updated.walHead).toBe(1024n);
    expect(updated.maxNodeId).toBe(100n);
    expect(updated.nextTxId).toBe(10n);
    expect(updated.lastCommitTs).toBeGreaterThan(0n);
  });

  test("updates header for compaction", () => {
    const original = createEmptyHeader();
    original.activeSnapshotGen = 5n;
    original.walHead = 1024n;
    original.walTail = 512n;
    
    const updated = updateHeaderForCompaction(
      original,
      100n,  // snapshotStartPage
      50n,   // snapshotPageCount
      6n,    // newGeneration
    );
    
    expect(updated.snapshotStartPage).toBe(100n);
    expect(updated.snapshotPageCount).toBe(50n);
    expect(updated.activeSnapshotGen).toBe(6n);
    expect(updated.prevSnapshotGen).toBe(5n);
    expect(updated.walHead).toBe(0n); // Reset after compaction
    expect(updated.walTail).toBe(0n);
  });

  test("calculates WAL area offset", () => {
    const header = createEmptyHeader(4096, 16n);
    expect(getWalAreaOffset(header)).toBe(4096); // After first page
  });

  test("calculates WAL area size", () => {
    const header = createEmptyHeader(4096, 16n);
    expect(getWalAreaSize(header)).toBe(16 * 4096);
  });
});

describe("Pager", () => {
  let tmpDir: string;
  let testFile: string;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "raydb-pager-test-"));
    testFile = join(tmpDir, "test.raydb");
  });

  afterEach(() => {
    try {
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  test("creates and opens pager", () => {
    const pager = createPager(testFile);
    expect(pager.pageSize).toBe(DEFAULT_PAGE_SIZE);
    expect(pager.fileSize).toBe(0);
    pager.close();
  });

  test("writes and reads pages", () => {
    const pager = createPager(testFile);
    
    const data = new Uint8Array(4096);
    data.fill(0xAB);
    data[0] = 0x12;
    data[4095] = 0x34;
    
    pager.writePage(0, data);
    
    const read = pager.readPage(0);
    expect(read[0]).toBe(0x12);
    expect(read[4095]).toBe(0x34);
    expect(read[100]).toBe(0xAB);
    
    pager.close();
  });

  test("allocates pages", () => {
    const pager = createPager(testFile);
    
    // Allocate 10 pages
    const startPage = pager.allocatePages(10);
    expect(startPage).toBe(0);
    expect(pager.fileSize).toBe(10 * 4096);
    
    // Allocate 5 more
    const nextPage = pager.allocatePages(5);
    expect(nextPage).toBe(10);
    expect(pager.fileSize).toBe(15 * 4096);
    
    pager.close();
  });

  test("calculates pages to store bytes", () => {
    expect(pagesToStore(0, 4096)).toBe(0);
    expect(pagesToStore(1, 4096)).toBe(1);
    expect(pagesToStore(4096, 4096)).toBe(1);
    expect(pagesToStore(4097, 4096)).toBe(2);
    expect(pagesToStore(8192, 4096)).toBe(2);
    expect(pagesToStore(10000, 4096)).toBe(3);
  });
});

describe("WAL Buffer", () => {
  let tmpDir: string;
  let testFile: string;
  let pager: FilePager;
  let header: ReturnType<typeof createEmptyHeader>;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "raydb-wal-test-"));
    testFile = join(tmpDir, "test.raydb");
    
    // Create pager with header and WAL area
    pager = createPager(testFile);
    header = createEmptyHeader(4096, 16n); // 64KB WAL
    
    // Write header
    const headerBuffer = serializeHeader(header);
    pager.writePage(0, headerBuffer);
    
    // Allocate WAL pages
    pager.allocatePages(16);
  });

  afterEach(() => {
    try {
      pager.close();
    } catch {
      // Ignore
    }
    try {
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  test("creates WAL buffer from header", () => {
    const walBuffer = createWalBuffer(pager, header);
    
    expect(walBuffer.getHead()).toBe(0n);
    expect(walBuffer.getTail()).toBe(0n);
    expect(walBuffer.getAvailableSpace()).toBeGreaterThan(0);
  });

  test("calculates WAL size from snapshot size", () => {
    // Small snapshot: use minimum 1MB
    expect(calculateWalSize(1000000)).toBe(1 * 1024 * 1024);
    
    // Large snapshot: use 10%
    expect(calculateWalSize(1000 * 1024 * 1024)).toBe(100 * 1024 * 1024);
  });

  test("converts WAL size to page count", () => {
    expect(walSizeToPageCount(64 * 1024 * 1024, 4096)).toBe(16384);
    expect(walSizeToPageCount(4096, 4096)).toBe(1);
    expect(walSizeToPageCount(5000, 4096)).toBe(2);
  });

  test("checks if record can be written", () => {
    const walBuffer = createWalBuffer(pager, header);
    
    // Small record should fit
    expect(walBuffer.canWrite(100)).toBe(true);
    
    // Record larger than WAL should not fit
    expect(walBuffer.canWrite(100 * 1024 * 1024)).toBe(false);
  });

  test("writes WAL record", () => {
    const walBuffer = createWalBuffer(pager, header);
    
    const record = {
      type: WalRecordType.BEGIN,
      txid: 1n,
      payload: new Uint8Array(0),
    };
    
    const newHead = walBuffer.writeRecord(record);
    expect(newHead).toBeGreaterThan(0);
    expect(walBuffer.getHead()).toBe(BigInt(newHead));
    expect(walBuffer.getUsedSpace()).toBeGreaterThan(0);
  });

  test("scans written records", () => {
    const walBuffer = createWalBuffer(pager, header);
    
    // Write multiple records
    walBuffer.writeRecord({
      type: WalRecordType.BEGIN,
      txid: 1n,
      payload: new Uint8Array(0),
    });
    
    walBuffer.writeRecord({
      type: WalRecordType.COMMIT,
      txid: 1n,
      payload: new Uint8Array(0),
    });
    
    const records = walBuffer.scanRecords();
    expect(records.length).toBe(2);
    expect(records[0]!.type).toBe(WalRecordType.BEGIN);
    expect(records[1]!.type).toBe(WalRecordType.COMMIT);
  });

  test("clears WAL buffer", () => {
    const walBuffer = createWalBuffer(pager, header);
    
    walBuffer.writeRecord({
      type: WalRecordType.BEGIN,
      txid: 1n,
      payload: new Uint8Array(0),
    });
    
    expect(walBuffer.getHead()).toBeGreaterThan(0n);
    
    walBuffer.clear();
    
    expect(walBuffer.getHead()).toBe(0n);
    expect(walBuffer.getTail()).toBe(0n);
  });
});

describe("Single-File Database Path Detection", () => {
  test("detects single-file paths", () => {
    expect(isSingleFilePath("test.raydb")).toBe(true);
    expect(isSingleFilePath("/path/to/db.raydb")).toBe(true);
    expect(isSingleFilePath("C:\\path\\db.raydb")).toBe(true);
  });

  test("rejects non-single-file paths", () => {
    expect(isSingleFilePath("test")).toBe(false);
    expect(isSingleFilePath("test.db")).toBe(false);
    expect(isSingleFilePath("/path/to/db")).toBe(false);
    expect(isSingleFilePath("test.raydb.bak")).toBe(false);
  });
});

describe("Single-File Database Lifecycle", () => {
  let tmpDir: string;
  let testFile: string;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "raydb-single-test-"));
    testFile = join(tmpDir, "test.raydb");
  });

  afterEach(() => {
    try {
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  test("creates new database", async () => {
    const db = await openSingleFileDB(testFile);
    
    expect(db.path).toBe(testFile);
    expect(db.readOnly).toBe(false);
    expect(db._header.version).toBe(VERSION_SINGLE_FILE);
    expect(db._nextNodeId).toBe(1);
    expect(db._nextTxId).toBe(1n);
    
    await closeSingleFileDB(db);
  });

  test("opens existing database", async () => {
    // Create database
    const db1 = await openSingleFileDB(testFile);
    await closeSingleFileDB(db1);
    
    // Reopen it
    const db2 = await openSingleFileDB(testFile);
    expect(db2._header.version).toBe(VERSION_SINGLE_FILE);
    
    await closeSingleFileDB(db2);
  });

  test("fails to open non-existent database without createIfMissing", async () => {
    await expect(
      openSingleFileDB(testFile, { createIfMissing: false })
    ).rejects.toThrow(/does not exist/);
  });

  test("fails to create database in read-only mode", async () => {
    await expect(
      openSingleFileDB(testFile, { readOnly: true })
    ).rejects.toThrow(/read-only/);
  });

  test("opens database in read-only mode", async () => {
    // Create database first
    const db1 = await openSingleFileDB(testFile);
    await closeSingleFileDB(db1);
    
    // Open in read-only mode
    const db2 = await openSingleFileDB(testFile, { readOnly: true });
    expect(db2.readOnly).toBe(true);
    
    await closeSingleFileDB(db2);
  });

  test("creates database with custom page size", async () => {
    const db = await openSingleFileDB(testFile, { pageSize: 8192 });
    
    expect(db._header.pageSize).toBe(8192);
    
    await closeSingleFileDB(db);
  });

  test("rejects invalid page size", async () => {
    await expect(
      openSingleFileDB(testFile, { pageSize: 5000 })
    ).rejects.toThrow(/Invalid page size/);
  });
});

describe("Single-File Compaction", () => {
  let tmpDir: string;
  let testFile: string;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "raydb-compact-test-"));
    testFile = join(tmpDir, "test.raydb");
  });

  afterEach(() => {
    try {
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  test("compacts empty database", async () => {
    const { optimizeSingleFile } = await import("../src/core/single-file-compactor.ts");
    
    const db = await openSingleFileDB(testFile);
    
    // Compact empty database should work
    await optimizeSingleFile(db);
    
    // Header should be updated
    expect(db._header.activeSnapshotGen).toBe(1n);
    
    await closeSingleFileDB(db);
  });

  test("compacts database with delta changes", async () => {
    const { optimizeSingleFile } = await import("../src/core/single-file-compactor.ts");
    const { createNode, defineLabel, defineEtype, addEdge, addNodeLabel } = await import("../src/core/delta.ts");
    
    const db = await openSingleFileDB(testFile);
    
    // Add some nodes and edges to delta
    defineLabel(db._delta, 1, "Person");
    defineEtype(db._delta, 1, "KNOWS");
    
    createNode(db._delta, 1, "alice");
    createNode(db._delta, 2, "bob");
    addNodeLabel(db._delta, 1, 1, true);
    addNodeLabel(db._delta, 2, 1, true);
    addEdge(db._delta, 1, 1, 2);
    
    db._nextNodeId = 3;
    
    // Compact
    await optimizeSingleFile(db);
    
    // Header should be updated
    expect(db._header.activeSnapshotGen).toBe(1n);
    expect(db._header.snapshotPageCount).toBeGreaterThan(0n);
    
    // Delta should be cleared
    expect(db._delta.createdNodes.size).toBe(0);
    expect(db._delta.outAdd.size).toBe(0);
    
    // Snapshot should be mmap'd
    expect(db._snapshotMmap).not.toBeNull();
    
    await closeSingleFileDB(db);
  });

  test("compacts database preserves data after reopen", async () => {
    const { optimizeSingleFile } = await import("../src/core/single-file-compactor.ts");
    const { createNode, defineLabel, defineEtype, addEdge, addNodeLabel } = await import("../src/core/delta.ts");
    const { parseSnapshot, getNodeId, getString, getOutEdges } = await import("../src/core/snapshot-reader.ts");
    
    let db = await openSingleFileDB(testFile);
    
    // Add nodes and edges
    defineLabel(db._delta, 1, "Person");
    defineEtype(db._delta, 1, "KNOWS");
    
    createNode(db._delta, 1, "alice");
    createNode(db._delta, 2, "bob");
    addNodeLabel(db._delta, 1, 1, true);
    addNodeLabel(db._delta, 2, 1, true);
    addEdge(db._delta, 1, 1, 2);
    
    db._nextNodeId = 3;
    
    // Compact
    await optimizeSingleFile(db);
    await closeSingleFileDB(db);
    
    // Reopen and verify
    db = await openSingleFileDB(testFile);
    
    expect(db._header.activeSnapshotGen).toBe(1n);
    expect(db._snapshotMmap).not.toBeNull();
    
    // Parse snapshot and verify data
    const snapshot = parseSnapshot(db._snapshotMmap!);
    expect(Number(snapshot.header.numNodes)).toBe(2);
    expect(Number(snapshot.header.numEdges)).toBe(1);
    
    // Verify nodes
    const node1Id = getNodeId(snapshot, 0);
    const node2Id = getNodeId(snapshot, 1);
    expect([node1Id, node2Id].sort()).toEqual([1, 2]);
    
    await closeSingleFileDB(db);
  });

  test("rejects compaction on read-only database", async () => {
    const { optimizeSingleFile } = await import("../src/core/single-file-compactor.ts");
    
    // Create database first
    let db = await openSingleFileDB(testFile);
    await closeSingleFileDB(db);
    
    // Open read-only
    db = await openSingleFileDB(testFile, { readOnly: true });
    
    await expect(optimizeSingleFile(db)).rejects.toThrow(/read-only/);
    
    await closeSingleFileDB(db);
  });

  test("increments snapshot generation on each compaction", async () => {
    const { optimizeSingleFile } = await import("../src/core/single-file-compactor.ts");
    const { createNode, defineLabel, addNodeLabel } = await import("../src/core/delta.ts");
    
    const db = await openSingleFileDB(testFile);
    
    // First compaction
    defineLabel(db._delta, 1, "Label1");
    createNode(db._delta, 1, "node1");
    addNodeLabel(db._delta, 1, 1, true);
    db._nextNodeId = 2;
    
    await optimizeSingleFile(db);
    expect(db._header.activeSnapshotGen).toBe(1n);
    expect(db._header.prevSnapshotGen).toBe(0n);
    
    // Second compaction
    defineLabel(db._delta, 2, "Label2");
    createNode(db._delta, 2, "node2");
    addNodeLabel(db._delta, 2, 2, true);
    db._nextNodeId = 3;
    
    await optimizeSingleFile(db);
    expect(db._header.activeSnapshotGen).toBe(2n);
    expect(db._header.prevSnapshotGen).toBe(1n);
    
    await closeSingleFileDB(db);
  });
});

describe("Single-File Vacuum", () => {
  let tmpDir: string;
  let testFile: string;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "raydb-vacuum-test-"));
    testFile = join(tmpDir, "test.raydb");
  });

  afterEach(() => {
    try {
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  test("vacuum on empty database does nothing", async () => {
    const { vacuumSingleFile } = await import("../src/core/single-file-compactor.ts");
    
    const db = await openSingleFileDB(testFile);
    const sizeBefore = (db._pager as any).fileSize;
    
    await vacuumSingleFile(db);
    
    const sizeAfter = (db._pager as any).fileSize;
    // Size should remain the same (just header + WAL)
    expect(sizeAfter).toBe(sizeBefore);
    
    await closeSingleFileDB(db);
  });

  test("vacuum rejects read-only database", async () => {
    const { vacuumSingleFile } = await import("../src/core/single-file-compactor.ts");
    
    // Create database first
    let db = await openSingleFileDB(testFile);
    await closeSingleFileDB(db);
    
    // Open read-only
    db = await openSingleFileDB(testFile, { readOnly: true });
    
    await expect(vacuumSingleFile(db)).rejects.toThrow(/read-only/);
    
    await closeSingleFileDB(db);
  });
});

describe("WAL Buffer Full Handling", () => {
  let tmpDir: string;
  let testFile: string;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "raydb-wal-full-test-"));
    testFile = join(tmpDir, "test.raydb");
  });

  afterEach(() => {
    try {
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  test("throws WalBufferFullError when WAL is exhausted", async () => {
    // Create database with very small WAL (minimum size)
    const db = await openSingleFileDB(testFile, {
      walSize: 4096 * 2, // Just 2 pages = 8KB
    });
    
    const pager = db._pager as FilePager;
    const walBuffer = createWalBuffer(pager, db._header);
    
    // Fill the WAL with large records until it's full
    let errorThrown = false;
    try {
      for (let i = 0; i < 1000; i++) {
        const largePayload = new Uint8Array(1024);
        largePayload.fill(i % 256);
        
        walBuffer.writeRecord({
          type: WalRecordType.BEGIN,
          txid: BigInt(i),
          payload: largePayload,
        });
      }
    } catch (e: any) {
      if (e instanceof WalBufferFullError) {
        errorThrown = true;
      } else {
        throw e;
      }
    }
    
    expect(errorThrown).toBe(true);
    
    await closeSingleFileDB(db);
  });

  test("canWrite returns false when record won't fit", async () => {
    const db = await openSingleFileDB(testFile, {
      walSize: 4096 * 4, // 16KB WAL
    });
    
    const pager = db._pager as FilePager;
    const walBuffer = createWalBuffer(pager, db._header);
    
    // Check that a huge record won't fit
    const hugeSize = 1024 * 1024; // 1MB
    expect(walBuffer.canWrite(hugeSize)).toBe(false);
    
    // Small record should fit
    expect(walBuffer.canWrite(100)).toBe(true);
    
    await closeSingleFileDB(db);
  });

  test("WAL buffer space is reclaimed after clear", async () => {
    const db = await openSingleFileDB(testFile, {
      walSize: 4096 * 4, // 16KB WAL
    });
    
    const pager = db._pager as FilePager;
    const walBuffer = createWalBuffer(pager, db._header);
    
    // Write some records
    for (let i = 0; i < 5; i++) {
      walBuffer.writeRecord({
        type: WalRecordType.BEGIN,
        txid: BigInt(i),
        payload: new Uint8Array(100),
      });
    }
    
    const usedBefore = walBuffer.getUsedSpace();
    expect(usedBefore).toBeGreaterThan(0);
    
    // Clear WAL
    walBuffer.clear();
    
    expect(walBuffer.getUsedSpace()).toBe(0);
    expect(walBuffer.getAvailableSpace()).toBeGreaterThan(usedBefore);
    
    await closeSingleFileDB(db);
  });
});

describe("Crash Recovery", () => {
  let tmpDir: string;
  let testFile: string;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "raydb-crash-test-"));
    testFile = join(tmpDir, "test.raydb");
  });

  afterEach(() => {
    try {
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  test("recovers uncommitted transactions from WAL on reopen", async () => {
    // Note: This test writes WAL records directly without using delta functions
    
    // Create database and write some WAL records
    let db = await openSingleFileDB(testFile);
    const pager = db._pager as FilePager;
    const walBuffer = createWalBuffer(pager, db._header);
    
    // Write a complete transaction to WAL
    walBuffer.writeRecord({
      type: WalRecordType.BEGIN,
      txid: 1n,
      payload: new Uint8Array(0),
    });
    
    // Define a label: labelId (4) + nameLen (4) + name
    const labelName = "Person";
    const labelPayload = new Uint8Array(8 + labelName.length);
    const labelView = new DataView(labelPayload.buffer);
    labelView.setUint32(0, 1, true); // labelId = 1
    labelView.setUint32(4, labelName.length, true); // nameLen
    labelPayload.set(new TextEncoder().encode(labelName), 8);
    
    walBuffer.writeRecord({
      type: WalRecordType.DEFINE_LABEL,
      txid: 1n,
      payload: labelPayload,
    });
    
    // Create a node
    const nodePayload = new Uint8Array(16 + 5); // nodeId (8) + keyLen (4) + numLabels (4) + "alice" (5)
    const nodeView = new DataView(nodePayload.buffer);
    nodeView.setBigUint64(0, 1n, true); // nodeId = 1
    nodeView.setUint32(8, 5, true); // keyLen = 5
    nodeView.setUint32(12, 1, true); // numLabels = 1
    nodePayload.set(new TextEncoder().encode("alice"), 16);
    
    walBuffer.writeRecord({
      type: WalRecordType.CREATE_NODE,
      txid: 1n,
      payload: nodePayload,
    });
    
    walBuffer.writeRecord({
      type: WalRecordType.COMMIT,
      txid: 1n,
      payload: new Uint8Array(0),
    });
    
    // Flush WAL buffer pending writes before updating header
    walBuffer.flushPendingWrites();
    
    // Update header with WAL position
    db._header.walHead = BigInt(walBuffer.getHead());
    const { serializeHeader } = await import("../src/core/header.ts");
    pager.writePage(0, serializeHeader(db._header));
    await pager.sync();
    
    // Simulate crash by not calling closeSingleFileDB properly
    pager.close();
    
    // Reopen - should recover from WAL
    db = await openSingleFileDB(testFile);
    
    // Node should be recovered in delta
    expect(db._delta.createdNodes.has(1)).toBe(true);
    expect(db._delta.newLabels.has(1)).toBe(true);
    expect(db._delta.newLabels.get(1)).toBe("Person");
    
    await closeSingleFileDB(db);
  });

  test("ignores incomplete transactions during recovery", async () => {
    // Create database and write incomplete WAL records
    let db = await openSingleFileDB(testFile);
    const pager = db._pager as FilePager;
    const walBuffer = createWalBuffer(pager, db._header);
    
    // Write a transaction without COMMIT
    walBuffer.writeRecord({
      type: WalRecordType.BEGIN,
      txid: 1n,
      payload: new Uint8Array(0),
    });
    
    // Define a label (but no commit)
    const labelPayload = new Uint8Array(8 + 4);
    const labelView = new DataView(labelPayload.buffer);
    labelView.setUint32(0, 1, true);
    labelPayload.set(new TextEncoder().encode("Test"), 8);
    
    walBuffer.writeRecord({
      type: WalRecordType.DEFINE_LABEL,
      txid: 1n,
      payload: labelPayload,
    });
    
    // Flush WAL buffer pending writes before updating header
    walBuffer.flushPendingWrites();
    
    // Update header
    db._header.walHead = BigInt(walBuffer.getHead());
    const { serializeHeader } = await import("../src/core/header.ts");
    pager.writePage(0, serializeHeader(db._header));
    await pager.sync();
    
    // Simulate crash
    pager.close();
    
    // Reopen - incomplete transaction should be ignored
    db = await openSingleFileDB(testFile);
    
    // Label should NOT be recovered (transaction wasn't committed)
    expect(db._delta.newLabels.has(1)).toBe(false);
    
    await closeSingleFileDB(db);
  });
});
