/**
 * Crash and corruption tests
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { parseHeader, getWalAreaOffset } from "../src/core/header.ts";
import {
  beginTx,
  closeGraphDB,
  commit,
  createNode,
  getNodeByKey,
  openGraphDB,
  optimizeSingleFile,
} from "../src/index.ts";

const dbOptions = {
  autoCheckpoint: false,
};

function getWalBounds(file: Uint8Array): { start: number; used: number } {
  const header = parseHeader(file.subarray(0, 4096));
  const walOffset = getWalAreaOffset(header);
  const used = Math.max(
    Number(header.walPrimaryHead),
    Number(header.walSecondaryHead),
    Number(header.walHead)
  );
  return { start: walOffset, used };
}

describe("WAL Truncation", () => {
  let testDir: string;
  let testPath: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-crash-test-"));
    testPath = join(testDir, "db.raydb");
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  test("truncated WAL record is ignored", async () => {
    // Create database with some data
    const db1 = await openGraphDB(testPath, dbOptions);

    const tx1 = beginTx(db1);
    createNode(tx1, { key: "node1" });
    await commit(tx1);

    const tx2 = beginTx(db1);
    createNode(tx2, { key: "node2" });
    await commit(tx2);

    await closeGraphDB(db1);

    // Truncate the WAL (remove last few bytes)
    const fileData = Buffer.from(await readFile(testPath));
    const { start: walStart, used } = getWalBounds(fileData);
    const truncStart = walStart + Math.max(used - 20, 8);
    fileData.fill(0, truncStart, walStart + used);
    await writeFile(testPath, fileData);

    // Reopen - should recover what it can
    const db2 = await openGraphDB(testPath, dbOptions);

    // First transaction should be recovered
    expect(getNodeByKey(db2, "node1")).not.toBeNull();

    // Second transaction may or may not be recovered depending on truncation point
    // But database should open successfully

    await closeGraphDB(db2);
  });

  test("completely corrupted WAL tail is ignored", async () => {
    const db1 = await openGraphDB(testPath, dbOptions);

    const tx = beginTx(db1);
    createNode(tx, { key: "safe-node" });
    await commit(tx);

    const tx2 = beginTx(db1);
    createNode(tx2, { key: "maybe-lost" });
    await commit(tx2);

    await closeGraphDB(db1);

    // Corrupt WAL tail in-place
    const fileData = Buffer.from(await readFile(testPath));
    const { start: walStart, used } = getWalBounds(fileData);
    const corruptAt = walStart + Math.max(used - 8, 8);
    fileData[corruptAt] ^= 0xff;
    await writeFile(testPath, fileData);

    // Reopen - should still work
    const db2 = await openGraphDB(testPath, dbOptions);

    expect(getNodeByKey(db2, "safe-node")).not.toBeNull();

    await closeGraphDB(db2);
  });
});

describe("Header Corruption", () => {
  let testDir: string;
  let testPath: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-manifest-test-"));
    testPath = join(testDir, "db.raydb");
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  test("corrupted header CRC is detected", async () => {
    const db1 = await openGraphDB(testPath, dbOptions);
    await closeGraphDB(db1);

    // Corrupt header
    const fileData = Buffer.from(await readFile(testPath));
    fileData[20] ^= 0xff; // Flip some bits in header
    await writeFile(testPath, fileData);

    // Opening should fail with CRC or version error
    await expect(openGraphDB(testPath)).rejects.toThrow();
  });
});

describe("Snapshot Corruption", () => {
  let testDir: string;
  let testPath: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-snap-corrupt-test-"));
    testPath = join(testDir, "db.raydb");
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  test("corrupted snapshot CRC is detected", async () => {
    const db1 = await openGraphDB(testPath, dbOptions);

    const tx = beginTx(db1);
    createNode(tx, { key: "test" });
    await commit(tx);

    await optimizeSingleFile(db1);
    await closeGraphDB(db1);

    // Corrupt snapshot inside the single-file database
    const fileData = Buffer.from(await readFile(testPath));
    const header = parseHeader(fileData.subarray(0, 4096));
    if (header.snapshotPageCount > 0n) {
      const snapshotOffset = Number(header.snapshotStartPage) * header.pageSize;
      fileData[snapshotOffset + 16] ^= 0xff;
      await writeFile(testPath, fileData);
      await expect(openGraphDB(testPath)).rejects.toThrow();
    }
  });
});

describe("Recovery Scenarios", () => {
  let testDir: string;
  let testPath: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-recovery-scenario-"));
    testPath = join(testDir, "db.raydb");
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  test("crash during transaction (no COMMIT)", async () => {
    const db1 = await openGraphDB(testPath, dbOptions);

    // Committed transaction
    const tx1 = beginTx(db1);
    createNode(tx1, { key: "committed" });
    await commit(tx1);

    // Start but don't commit
    const tx2 = beginTx(db1);
    createNode(tx2, { key: "uncommitted" });

    // Close without committing (simulates crash)
    await closeGraphDB(db1);

    // Reopen
    const db2 = await openGraphDB(testPath, dbOptions);

    // Committed data should be there
    expect(getNodeByKey(db2, "committed")).not.toBeNull();

    // Uncommitted data should not be there
    expect(getNodeByKey(db2, "uncommitted")).toBeNull();

    await closeGraphDB(db2);
  });

  test("recovery with multiple WAL segments", async () => {
    const db1 = await openGraphDB(testPath, dbOptions);

    // Create data
    const tx1 = beginTx(db1);
    createNode(tx1, { key: "before-compact" });
    await commit(tx1);

    // Compact (creates new WAL segment)
    await optimizeSingleFile(db1);

    // Create more data
    const tx2 = beginTx(db1);
    createNode(tx2, { key: "after-compact" });
    await commit(tx2);

    await closeGraphDB(db1);

    // Reopen
    const db2 = await openGraphDB(testPath, dbOptions);

    expect(getNodeByKey(db2, "before-compact")).not.toBeNull();
    expect(getNodeByKey(db2, "after-compact")).not.toBeNull();

    await closeGraphDB(db2);
  });

  test("many transactions recovery", async () => {
    const db1 = await openGraphDB(testPath, dbOptions);

    // Create many small transactions
    for (let i = 0; i < 100; i++) {
      const tx = beginTx(db1);
      createNode(tx, { key: `node-${i}` });
      await commit(tx);
    }

    await closeGraphDB(db1);

    // Reopen
    const db2 = await openGraphDB(testPath, dbOptions);

    // All nodes should be recovered
    for (let i = 0; i < 100; i++) {
      expect(getNodeByKey(db2, `node-${i}`)).not.toBeNull();
    }

    await closeGraphDB(db2);
  });
});
