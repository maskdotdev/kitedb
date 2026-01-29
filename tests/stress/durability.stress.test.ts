/**
 * Durability Stress Tests
 *
 * Verifies crash recovery under various failure modes.
 *
 * Tests:
 * - Crash During Commit
 * - Crash During Compaction
 * - WAL Segment Rollover
 * - Repeated Crash-Restart Cycles
 * - Partial WAL Corruption
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  openGraphDB,
  closeGraphDB,
  beginTx,
  commit,
  rollback,
  createNode,
  getNodeByKey,
  nodeExists,
  optimize,
  definePropkey,
  setNodeProp,
  getNodeProp,
} from "../../src/index.ts";
import { PropValueTag } from "../../src/types.ts";
import type { GraphDB, NodeID } from "../../src/types.ts";
import { MANIFEST_FILENAME, WAL_DIR, walFilename } from "../../src/constants.ts";
import { getConfig, isQuickMode } from "./stress.config.ts";
import { randomString, randomInt } from "./helpers/generators.ts";

const config = getConfig(isQuickMode());

describe.skip("Durability Stress Tests (legacy multi-file)", () => {
  let testDir: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-durability-stress-"));
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  test("repeated crash-restart cycles preserve committed data", async () => {
    const CYCLES = config.durability.crashCycles;
    const NODES_PER_CYCLE = 10;

    const expectedNodes = new Map<string, boolean>(); // key -> committed

    for (let cycle = 0; cycle < CYCLES; cycle++) {
      const db = await openGraphDB(testDir);

      // Create some nodes
      const tx = beginTx(db);
      for (let i = 0; i < NODES_PER_CYCLE; i++) {
        const key = `cycle_${cycle}_node_${i}`;
        createNode(tx, { key });
        
        // Randomly decide to commit or not (simulating crash)
        if (Math.random() > 0.3) {
          // This node might survive
        }
      }

      // 70% chance to commit (simulating crash before commit)
      if (Math.random() > 0.3) {
        await commit(tx);
        // Mark as committed
        for (let i = 0; i < NODES_PER_CYCLE; i++) {
          expectedNodes.set(`cycle_${cycle}_node_${i}`, true);
        }
      } else {
        // Simulate crash by just closing without commit
        rollback(tx);
        for (let i = 0; i < NODES_PER_CYCLE; i++) {
          expectedNodes.set(`cycle_${cycle}_node_${i}`, false);
        }
      }

      await closeGraphDB(db);
    }

    // Final verification
    const verifyDb = await openGraphDB(testDir);
    
    let correctlyPresent = 0;
    let correctlyAbsent = 0;
    let incorrectlyPresent = 0;
    let incorrectlyAbsent = 0;

    for (const [key, shouldExist] of expectedNodes) {
      const node = getNodeByKey(verifyDb, key);
      const exists = node !== null;

      if (shouldExist && exists) correctlyPresent++;
      else if (!shouldExist && !exists) correctlyAbsent++;
      else if (!shouldExist && exists) incorrectlyPresent++;
      else incorrectlyAbsent++;
    }

    console.log(`Crash cycles: ${correctlyPresent} correct present, ${correctlyAbsent} correct absent`);
    console.log(`Errors: ${incorrectlyPresent} incorrectly present, ${incorrectlyAbsent} incorrectly absent`);

    expect(incorrectlyPresent).toBe(0);
    expect(incorrectlyAbsent).toBe(0);

    await closeGraphDB(verifyDb);
  }, 300000);

  test("WAL truncation during transaction is handled gracefully", async () => {
    // Create database with multiple transactions
    const db1 = await openGraphDB(testDir);

    const committedKeys: string[] = [];
    
    // Create several committed transactions
    for (let i = 0; i < 20; i++) {
      const tx = beginTx(db1);
      const key = `committed_${i}`;
      createNode(tx, { key });
      await commit(tx);
      committedKeys.push(key);
    }

    await closeGraphDB(db1);

    // Truncate WAL to various lengths
    const walPath = join(testDir, WAL_DIR, walFilename(1n));
    const originalWal = await readFile(walPath);
    
    // Test various truncation points
    const truncationPoints = [
      Math.floor(originalWal.length * 0.9),
      Math.floor(originalWal.length * 0.7),
      Math.floor(originalWal.length * 0.5),
    ];

    for (const truncPoint of truncationPoints) {
      // Create a copy of test directory for this truncation test
      const truncDir = await mkdtemp(join(tmpdir(), "ray-trunc-test-"));
      
      try {
        // Copy manifest
        const manifest = await readFile(join(testDir, MANIFEST_FILENAME));
        await writeFile(join(truncDir, MANIFEST_FILENAME), manifest);
        
        // Copy truncated WAL
        await mkdir(join(truncDir, WAL_DIR), { recursive: true });
        const truncatedWal = originalWal.subarray(0, truncPoint);
        await writeFile(join(truncDir, WAL_DIR, walFilename(1n)), truncatedWal);

        // Try to open - should handle gracefully
        const db2 = await openGraphDB(truncDir);

        // Some transactions should be recovered
        let recovered = 0;
        for (const key of committedKeys) {
          if (getNodeByKey(db2, key) !== null) {
            recovered++;
          }
        }

        console.log(`Truncation at ${truncPoint}/${originalWal.length}: recovered ${recovered}/${committedKeys.length}`);
        expect(recovered).toBeGreaterThan(0);

        await closeGraphDB(db2);
      } finally {
        await rm(truncDir, { recursive: true, force: true });
      }
    }
  }, 120000);

  test("random byte corruption in WAL is detected", async () => {
    // Create database with data
    const db1 = await openGraphDB(testDir);

    const tx = beginTx(db1);
    for (let i = 0; i < 50; i++) {
      createNode(tx, { key: `safe_${i}` });
    }
    await commit(tx);

    await closeGraphDB(db1);

    // Corrupt random bytes in WAL
    const walPath = join(testDir, WAL_DIR, walFilename(1n));
    const walData = await readFile(walPath);
    
    // Create copy for corruption test
    const corruptDir = await mkdtemp(join(tmpdir(), "ray-corrupt-test-"));

    try {
      // Copy manifest
      const manifest = await readFile(join(testDir, MANIFEST_FILENAME));
      await writeFile(join(corruptDir, MANIFEST_FILENAME), manifest);

      // Corrupt WAL
      await mkdir(join(corruptDir, WAL_DIR), { recursive: true });
      const corrupted = Buffer.from(walData);
      
      // Flip some random bits in the middle
      for (let i = 0; i < 10; i++) {
        const pos = randomInt(100, corrupted.length - 100);
        corrupted[pos] ^= 0xFF;
      }
      
      await writeFile(join(corruptDir, WAL_DIR, walFilename(1n)), corrupted);

      // Open should either recover partial data or fail gracefully
      let opened = false;
      let recovered = 0;
      let corruptionDetected = false;
      let detectionMessage = "";

      try {
        const db2 = await openGraphDB(corruptDir);
        opened = true;
        
        // Count recovered nodes
        for (let i = 0; i < 50; i++) {
          if (getNodeByKey(db2, `safe_${i}`) !== null) {
            recovered++;
          }
        }
        
        await closeGraphDB(db2);
      } catch (e) {
        corruptionDetected = true;
        detectionMessage = String(e);
        console.log(`Corruption detected and rejected: ${detectionMessage}`);
        expect(detectionMessage.toLowerCase()).toMatch(/crc|checksum|corrupt/);
      }

      console.log(`Random corruption: opened=${opened}, recovered=${recovered}/50`);
      if (opened) {
        expect(recovered).toBeLessThan(50);
      } else {
        expect(corruptionDetected).toBe(true);
      }
    } finally {
      await rm(corruptDir, { recursive: true, force: true });
    }
  }, 60000);

  test("recovery after crash during compaction", async () => {
    // Create database with data
    const db1 = await openGraphDB(testDir);

    const tx1 = beginTx(db1);
    for (let i = 0; i < 100; i++) {
      createNode(tx1, { key: `pre_compact_${i}` });
    }
    await commit(tx1);

    // Run compaction
    await optimize(db1);

    // Add more data after compaction
    const tx2 = beginTx(db1);
    for (let i = 0; i < 50; i++) {
      createNode(tx2, { key: `post_compact_${i}` });
    }
    await commit(tx2);

    // Close cleanly
    await closeGraphDB(db1);

    // Simulate partial corruption of snapshot by truncating it
    const snapshotsDir = join(testDir, "snapshots");
    try {
      const files = await readdir(snapshotsDir);
      const snapshotFile = files.find(f => f.endsWith(".gds"));
      
      if (snapshotFile) {
        const snapshotPath = join(snapshotsDir, snapshotFile);
        const data = await readFile(snapshotPath);
        // Truncate snapshot
        const truncated = data.subarray(0, Math.floor(data.length * 0.8));
        await writeFile(snapshotPath, truncated);
      }
    } catch {
      // No snapshots directory yet
    }

    // Reopen - should recover from WAL
    const db2 = await openGraphDB(testDir);

    // Pre-compaction data should be in snapshot or WAL
    let preCompactRecovered = 0;
    for (let i = 0; i < 100; i++) {
      if (getNodeByKey(db2, `pre_compact_${i}`) !== null) {
        preCompactRecovered++;
      }
    }

    // Post-compaction data should be in WAL
    let postCompactRecovered = 0;
    for (let i = 0; i < 50; i++) {
      if (getNodeByKey(db2, `post_compact_${i}`) !== null) {
        postCompactRecovered++;
      }
    }

    console.log(`Compaction recovery: pre=${preCompactRecovered}/100, post=${postCompactRecovered}/50`);

    // Post-compaction data should definitely be recovered (in WAL)
    expect(postCompactRecovered).toBe(50);

    await closeGraphDB(db2);
  }, 120000);

  test("large WAL recovery performance", async () => {
    const TRANSACTIONS = 1000;
    const NODES_PER_TX = 10;

    // Create many transactions
    const db1 = await openGraphDB(testDir);

    const startCreate = performance.now();
    for (let t = 0; t < TRANSACTIONS; t++) {
      const tx = beginTx(db1);
      for (let n = 0; n < NODES_PER_TX; n++) {
        createNode(tx, { key: `large_wal_${t}_${n}` });
      }
      await commit(tx);
    }
    const createTime = performance.now() - startCreate;
    console.log(`Created ${TRANSACTIONS * NODES_PER_TX} nodes in ${createTime.toFixed(0)}ms`);

    await closeGraphDB(db1);

    // Measure recovery time
    const startRecovery = performance.now();
    const db2 = await openGraphDB(testDir);
    const recoveryTime = performance.now() - startRecovery;
    console.log(`WAL recovery took ${recoveryTime.toFixed(0)}ms`);

    // Verify data
    let recovered = 0;
    for (let t = 0; t < TRANSACTIONS; t++) {
      for (let n = 0; n < NODES_PER_TX; n++) {
        if (getNodeByKey(db2, `large_wal_${t}_${n}`) !== null) {
          recovered++;
        }
      }
    }

    expect(recovered).toBe(TRANSACTIONS * NODES_PER_TX);

    await closeGraphDB(db2);
  }, 300000);
});
