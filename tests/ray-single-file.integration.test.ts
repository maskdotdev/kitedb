/**
 * Integration tests for single-file Ray API
 * Tests that openGraphDB/closeGraphDB correctly use single-file format
 */

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";

import {
  openGraphDB,
  closeGraphDB,
  beginTx,
  commit,
  rollback,
  createNode,
  deleteNode,
  getNodeByKey,
  nodeExists,
  addEdge,
  deleteEdge,
  getNeighborsOut,
  getNeighborsIn,
  edgeExists,
  setNodeProp,
  getNodeProp,
  defineLabel,
  defineEtype,
  definePropkey,
  stats,
  PropValueTag,
  type GraphDB,
} from "../src/index.ts";

describe("Single-File Ray API Integration", () => {
  let tmpDir: string;
  let db: GraphDB;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "raydb-single-file-test-"));
  });

  afterEach(async () => {
    if (db) {
      await closeGraphDB(db);
    }
    await rm(tmpDir, { recursive: true, force: true });
  });

  describe("Database Creation", () => {
    test("creates .raydb file when path has no extension", async () => {
      const dbPath = join(tmpDir, "test");
      db = await openGraphDB(dbPath);
      
      expect(db._isSingleFile).toBe(true);
      expect(existsSync(dbPath + ".raydb")).toBe(true);
      expect(db._pager).not.toBeNull();
      expect(db._header).not.toBeNull();
    });

    test("creates .raydb file when path ends with .raydb", async () => {
      const dbPath = join(tmpDir, "test.raydb");
      db = await openGraphDB(dbPath);
      
      expect(db._isSingleFile).toBe(true);
      expect(existsSync(dbPath)).toBe(true);
    });
  });

  describe("Basic Node Operations", () => {
    test("creates and retrieves nodes with keys", async () => {
      db = await openGraphDB(join(tmpDir, "test"));
      
      const tx = beginTx(db);
      const nodeId = createNode(tx, { key: "user:1" });
      await commit(tx);
      
      const found = getNodeByKey(db, "user:1");
      expect(found).toBe(nodeId);
    });

    test("node exists check works", async () => {
      db = await openGraphDB(join(tmpDir, "test"));
      
      const tx = beginTx(db);
      const nodeId = createNode(tx, {});
      expect(nodeExists(tx, nodeId)).toBe(true);
      await commit(tx);
      
      expect(nodeExists(db, nodeId)).toBe(true);
      expect(nodeExists(db, 99999)).toBe(false);
    });

    test("deletes nodes", async () => {
      db = await openGraphDB(join(tmpDir, "test"));
      
      const tx1 = beginTx(db);
      const nodeId = createNode(tx1, { key: "temp" });
      await commit(tx1);
      
      expect(nodeExists(db, nodeId)).toBe(true);
      
      const tx2 = beginTx(db);
      deleteNode(tx2, nodeId);
      await commit(tx2);
      
      expect(nodeExists(db, nodeId)).toBe(false);
    });
  });

  describe("Edge Operations", () => {
    test("adds and queries edges", async () => {
      db = await openGraphDB(join(tmpDir, "test"));
      
      const tx1 = beginTx(db);
      const etypeId = defineEtype(tx1, "follows");
      const node1 = createNode(tx1, { key: "alice" });
      const node2 = createNode(tx1, { key: "bob" });
      await commit(tx1);
      
      const tx2 = beginTx(db);
      addEdge(tx2, node1, etypeId, node2);
      await commit(tx2);
      
      expect(edgeExists(db, node1, etypeId, node2)).toBe(true);
      
      const neighbors = [...getNeighborsOut(db, node1, etypeId)];
      expect(neighbors.length).toBe(1);
      expect(neighbors[0]!.dst).toBe(node2);
    });

    test("deletes edges", async () => {
      db = await openGraphDB(join(tmpDir, "test"));
      
      const tx1 = beginTx(db);
      const etypeId = defineEtype(tx1, "follows");
      const node1 = createNode(tx1, { key: "alice" });
      const node2 = createNode(tx1, { key: "bob" });
      addEdge(tx1, node1, etypeId, node2);
      await commit(tx1);
      
      expect(edgeExists(db, node1, etypeId, node2)).toBe(true);
      
      const tx2 = beginTx(db);
      deleteEdge(tx2, node1, etypeId, node2);
      await commit(tx2);
      
      expect(edgeExists(db, node1, etypeId, node2)).toBe(false);
    });
  });

  describe("Property Operations", () => {
    test("sets and gets node properties", async () => {
      db = await openGraphDB(join(tmpDir, "test"));
      
      const tx1 = beginTx(db);
      const propKeyId = definePropkey(tx1, "name");
      const nodeId = createNode(tx1, {});
      setNodeProp(tx1, nodeId, propKeyId, { tag: PropValueTag.STRING, value: "Alice" });
      await commit(tx1);
      
      const prop = getNodeProp(db, nodeId, propKeyId);
      expect(prop).not.toBeNull();
      expect(prop?.tag).toBe(PropValueTag.STRING);
      if (prop?.tag === PropValueTag.STRING) {
        expect(prop.value).toBe("Alice");
      }
    });
  });

  describe("Transaction Rollback", () => {
    test("rollback discards changes", async () => {
      db = await openGraphDB(join(tmpDir, "test"));
      
      const tx = beginTx(db);
      const nodeId = createNode(tx, { key: "temp" });
      expect(nodeExists(tx, nodeId)).toBe(true);
      
      rollback(tx);
      
      expect(getNodeByKey(db, "temp")).toBeNull();
    });
  });

  describe("Data Persistence", () => {
    test("data persists after close and reopen", async () => {
      const dbPath = join(tmpDir, "persist");
      
      // Create and populate database
      db = await openGraphDB(dbPath);
      const tx = beginTx(db);
      const propKeyId = definePropkey(tx, "name");
      const nodeId = createNode(tx, { key: "user:1" });
      setNodeProp(tx, nodeId, propKeyId, { tag: PropValueTag.STRING, value: "Bob" });
      await commit(tx);
      await closeGraphDB(db);
      
      // Reopen and verify
      db = await openGraphDB(dbPath);
      const found = getNodeByKey(db, "user:1");
      expect(found).not.toBeNull();
      
      // Note: propKeyId may be different after reopen - need to look it up
      const dbStats = stats(db);
      expect(dbStats.deltaNodesCreated).toBe(1);
    });
  });

  describe("Stats", () => {
    test("returns correct stats for single-file database", async () => {
      db = await openGraphDB(join(tmpDir, "test"));
      
      const tx = beginTx(db);
      const etypeId = defineEtype(tx, "knows");
      createNode(tx, { key: "a" });
      createNode(tx, { key: "b" });
      await commit(tx);
      
      const dbStats = stats(db);
      expect(dbStats.deltaNodesCreated).toBe(2);
      expect(dbStats.snapshotGen).toBe(0n); // No checkpoint yet
    });
  });

  describe.skip("Existing Directory Detection (legacy multi-file)", () => {
    test("opens existing directory database as multi-file", async () => {
      // Create a fake multi-file database structure
      const { mkdir, writeFile } = await import("node:fs/promises");
      const dbPath = join(tmpDir, "multi-file-db");
      await mkdir(dbPath, { recursive: true });
      await mkdir(join(dbPath, "snapshots"), { recursive: true });
      await mkdir(join(dbPath, "wal"), { recursive: true });
      
      // Create a proper manifest file (84 bytes)
      // Structure: magic(4) + version(4) + minReader(4) + reserved(4) + 
      //           activeSnapshotGen(8) + prevSnapshotGen(8) + activeWalSeg(8) +
      //           reserved2[5](40) + crc32c(4) = 84 bytes
      const manifest = new Uint8Array(84);
      const view = new DataView(manifest.buffer);
      view.setUint32(0, 0x4d424447, true); // Magic "GDBM"
      view.setUint32(4, 1, true); // version
      view.setUint32(8, 1, true); // minReaderVersion
      view.setUint32(12, 0, true); // reserved
      view.setBigUint64(16, 0n, true); // activeSnapshotGen
      view.setBigUint64(24, 0n, true); // prevSnapshotGen
      view.setBigUint64(32, 1n, true); // activeWalSeg
      // reserved2[5] at offset 40-80 (already zeroed)
      // Calculate CRC32C of first 80 bytes
      const { crc32c } = await import("../src/util/crc.ts");
      const crc = crc32c(manifest.subarray(0, 80));
      view.setUint32(80, crc, true);
      
      await writeFile(join(dbPath, "manifest.gdm"), manifest);
      
      // Open database - should detect as multi-file
      db = await openGraphDB(dbPath);
      expect(db._isSingleFile).toBe(false);
      expect(db._manifest).not.toBeNull();
    });
  });

  describe("Auto-Checkpoint Option", () => {
    test("respects autoCheckpoint option", async () => {
      db = await openGraphDB(join(tmpDir, "test"), {
        autoCheckpoint: true,
        checkpointThreshold: 0.8,
      });
      
      expect(db._autoCheckpoint).toBe(true);
      expect(db._checkpointThreshold).toBe(0.8);
    });

    test("autoCheckpoint defaults to true", async () => {
      db = await openGraphDB(join(tmpDir, "test"));
      
      expect(db._autoCheckpoint).toBe(true);
    });
  });
});
