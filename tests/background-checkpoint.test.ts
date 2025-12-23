/**
 * Tests for background checkpointing with dual-region WAL
 */

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { existsSync, unlinkSync, mkdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { openGraphDB, closeGraphDB, beginTx, commit } from "../src/index.ts";
import type { GraphDB } from "../src/types.ts";
import { 
  shouldCheckpoint, 
  triggerBackgroundCheckpoint, 
  isCheckpointRunning,
  getCheckpointPromise,
  checkpoint,
} from "../src/ray/graph-db/checkpoint.ts";
import { createWalBuffer } from "../src/core/wal-buffer.ts";
import type { FilePager } from "../src/core/pager.ts";
import { createNode, setNodeProp } from "../src/ray/graph-db/nodes.ts";
import { defineLabel, definePropkey } from "../src/ray/graph-db/definitions.ts";
import { PropValueTag } from "../src/types.ts";

const TEST_DIR = join(tmpdir(), "raydb-background-checkpoint-tests");

// Helper to clean up test files
function cleanupTestFile(path: string): void {
  if (existsSync(path)) {
    try {
      unlinkSync(path);
    } catch {
      // Ignore errors
    }
  }
}

describe("Background Checkpoint", () => {
  beforeEach(() => {
    if (!existsSync(TEST_DIR)) {
      mkdirSync(TEST_DIR, { recursive: true });
    }
  });

  afterEach(() => {
    // Clean up test directory
    try {
      rmSync(TEST_DIR, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  describe("Dual-Region WAL Buffer", () => {
    test("should have correct region sizes (75/25 split)", async () => {
      const dbPath = join(TEST_DIR, "region-test.raydb");
      cleanupTestFile(dbPath);

      const db = await openGraphDB(dbPath, { 
        walSize: 64 * 1024, // 64KB WAL
      });

      const pager = db._pager as FilePager;
      const walBuffer = createWalBuffer(pager, db._header!);

      // Primary region should be ~75% of WAL
      const primarySize = walBuffer.getPrimaryRegionSize();
      const secondarySize = walBuffer.getSecondaryRegionSize();
      const totalSize = Number(db._header!.walPageCount) * db._header!.pageSize;

      expect(primarySize).toBeGreaterThan(0);
      expect(secondarySize).toBeGreaterThan(0);
      expect(primarySize + secondarySize).toBe(totalSize);
      
      // Check 75/25 ratio (with some tolerance)
      const primaryRatio = primarySize / totalSize;
      expect(primaryRatio).toBeGreaterThan(0.7);
      expect(primaryRatio).toBeLessThan(0.8);

      await closeGraphDB(db);
    });

    test("should switch between regions", async () => {
      const dbPath = join(TEST_DIR, "switch-test.raydb");
      cleanupTestFile(dbPath);

      const db = await openGraphDB(dbPath, { 
        walSize: 64 * 1024,
      });

      const pager = db._pager as FilePager;
      const walBuffer = createWalBuffer(pager, db._header!);

      // Initially should be in primary region
      expect(walBuffer.getActiveRegion()).toBe(0);

      // Switch to secondary
      walBuffer.switchToSecondary();
      expect(walBuffer.getActiveRegion()).toBe(1);

      // Switch back to primary
      walBuffer.switchToPrimary();
      expect(walBuffer.getActiveRegion()).toBe(0);

      await closeGraphDB(db);
    });
  });

  describe("Checkpoint State Management", () => {
    test("should report correct checkpoint state", async () => {
      const dbPath = join(TEST_DIR, "state-test.raydb");
      cleanupTestFile(dbPath);

      const db = await openGraphDB(dbPath, { 
        autoCheckpoint: false, // Disable auto-checkpoint for manual testing
      });

      // Initially not running
      expect(isCheckpointRunning(db)).toBe(false);
      expect(getCheckpointPromise(db)).toBeNull();

      await closeGraphDB(db);
    });

    test("should handle concurrent checkpoint requests", async () => {
      const dbPath = join(TEST_DIR, "concurrent-test.raydb");
      cleanupTestFile(dbPath);

      const db = await openGraphDB(dbPath, {
        autoCheckpoint: false,
        walSize: 64 * 1024,
      });

      // Add some data first - define labels/props in one tx, then use them
      let txHandle = beginTx(db);
      const label = defineLabel(txHandle, "Person");
      const prop = definePropkey(txHandle, "name");
      await commit(txHandle);
      
      txHandle = beginTx(db);
      for (let i = 0; i < 100; i++) {
        const node = createNode(txHandle, { labels: [label] });
        setNodeProp(txHandle, node, prop, { tag: PropValueTag.STRING, value: `Person ${i}` });
      }
      await commit(txHandle);

      // Trigger multiple checkpoints - should return same promise
      const promise1 = triggerBackgroundCheckpoint(db);
      const promise2 = triggerBackgroundCheckpoint(db);
      
      expect(promise1).toBe(promise2);

      // Wait for checkpoint to complete
      await promise1;

      // Should be idle after completion
      expect(isCheckpointRunning(db)).toBe(false);

      await closeGraphDB(db);
    });
  });

  describe("Writes During Checkpoint", () => {
    test("should allow writes while checkpoint is running", async () => {
      const dbPath = join(TEST_DIR, "writes-during-test.raydb");
      cleanupTestFile(dbPath);

      const db = await openGraphDB(dbPath, {
        autoCheckpoint: false,
        walSize: 256 * 1024, // Larger WAL for more writes
      });

      // Define labels and props first
      let txHandle = beginTx(db);
      const label = defineLabel(txHandle, "Item");
      const prop = definePropkey(txHandle, "value");
      await commit(txHandle);

      // Add initial data
      txHandle = beginTx(db);
      for (let i = 0; i < 50; i++) {
        const node = createNode(txHandle, { labels: [label] });
        setNodeProp(txHandle, node, prop, { tag: PropValueTag.I64, value: BigInt(i) });
      }
      await commit(txHandle);

      // Start checkpoint (non-blocking)
      const checkpointPromise = triggerBackgroundCheckpoint(db);

      // Write more data while checkpoint is running
      txHandle = beginTx(db);
      for (let i = 0; i < 20; i++) {
        const node = createNode(txHandle, { labels: [label] });
        setNodeProp(txHandle, node, prop, { tag: PropValueTag.I64, value: BigInt(1000 + i) });
      }
      await commit(txHandle);

      // Wait for checkpoint to complete
      await checkpointPromise;

      // Verify all data is accessible
      expect(db._nextNodeId).toBeGreaterThanOrEqual(70);

      await closeGraphDB(db);
    });
  });

  describe("Backpressure", () => {
    test("should trigger checkpoint at threshold", async () => {
      const dbPath = join(TEST_DIR, "threshold-test.raydb");
      cleanupTestFile(dbPath);

      // Use a small WAL size to trigger checkpoint quickly
      // 32KB total = 24KB primary (75%) + 8KB secondary (25%)
      // At 50% threshold, checkpoint triggers at ~12KB primary usage
      const db = await openGraphDB(dbPath, {
        autoCheckpoint: true,
        checkpointThreshold: 0.5, // Trigger at 50% for faster test
        walSize: 32 * 1024, // 32KB WAL (24KB primary, 8KB secondary)
      });

      // Create definitions first
      let txHandle = beginTx(db);
      const label = defineLabel(txHandle, "Test");
      const prop = definePropkey(txHandle, "data");
      await commit(txHandle);

      // Write until checkpoint triggers
      // Each node with property is ~120-150 bytes, so 10 nodes per batch = ~1.4KB
      // Need ~9 batches to hit 50% of 24KB
      let checkpointTriggered = false;
      for (let batch = 0; batch < 50; batch++) {
        txHandle = beginTx(db);
        // Create fewer, larger nodes to fill WAL faster
        for (let i = 0; i < 5; i++) {
          const node = createNode(txHandle, { labels: [label] });
          setNodeProp(txHandle, node, prop, { tag: PropValueTag.STRING, value: "x".repeat(200) });
        }
        await commit(txHandle);

        // Check if checkpoint was triggered (running) or already completed (activeSnapshotGen > 0)
        if (isCheckpointRunning(db) || db._header!.activeSnapshotGen > 0n) {
          checkpointTriggered = true;
          break;
        }
      }

      expect(checkpointTriggered).toBe(true);
      
      // Wait for any running checkpoint to complete
      const promise = getCheckpointPromise(db);
      if (promise) {
        await promise;
      }

      await closeGraphDB(db);
    });
  });

  describe("Crash Recovery", () => {
    test("should recover from crash during checkpoint", async () => {
      const dbPath = join(TEST_DIR, "crash-recovery-test.raydb");
      cleanupTestFile(dbPath);

      // Create database and add data
      let db = await openGraphDB(dbPath, {
        autoCheckpoint: false,
        walSize: 64 * 1024,
      });

      // Add data - define first, then use
      let txHandle = beginTx(db);
      const label = defineLabel(txHandle, "Node");
      const prop = definePropkey(txHandle, "id");
      await commit(txHandle);
      
      txHandle = beginTx(db);
      const createdNodes: number[] = [];
      for (let i = 0; i < 10; i++) {
        const node = createNode(txHandle, { labels: [label] });
        setNodeProp(txHandle, node, prop, { tag: PropValueTag.I64, value: BigInt(i) });
        createdNodes.push(node);
      }
      await commit(txHandle);

      // Simulate crash by setting checkpointInProgress flag
      // This tests the recovery path
      const pager = db._pager as FilePager;
      const header = {
        ...db._header!,
        checkpointInProgress: 1 as 0 | 1,
      };
      const { writeHeader } = await import("../src/core/header.ts");
      await writeHeader(pager, header);
      db._header = header;

      await closeGraphDB(db);

      // Reopen - should recover from interrupted checkpoint
      db = await openGraphDB(dbPath, {
        autoCheckpoint: false,
      });

      // checkpointInProgress should be cleared
      expect(db._header!.checkpointInProgress).toBe(0);
      
      // Data should still be accessible (replayed from WAL)
      expect(db._nextNodeId).toBeGreaterThanOrEqual(10);

      await closeGraphDB(db);
    });
  });

  describe("Blocking Checkpoint Fallback", () => {
    test("blocking checkpoint should wait for background checkpoint", async () => {
      const dbPath = join(TEST_DIR, "blocking-fallback-test.raydb");
      cleanupTestFile(dbPath);

      const db = await openGraphDB(dbPath, {
        autoCheckpoint: false,
        walSize: 64 * 1024,
      });

      // Add data
      let txHandle = beginTx(db);
      const label = defineLabel(txHandle, "Test");
      await commit(txHandle);
      
      txHandle = beginTx(db);
      for (let i = 0; i < 10; i++) {
        createNode(txHandle, { labels: [label] });
      }
      await commit(txHandle);

      // Start background checkpoint
      const bgPromise = triggerBackgroundCheckpoint(db);

      // Immediately call blocking checkpoint - should wait for background to complete
      await checkpoint(db);

      // Both should complete without error
      await bgPromise;

      await closeGraphDB(db);
    });
  });
});

describe("WalBuffer Dual-Region Operations", () => {
  beforeEach(() => {
    if (!existsSync(TEST_DIR)) {
      mkdirSync(TEST_DIR, { recursive: true });
    }
  });
  
  afterEach(() => {
    try {
      rmSync(TEST_DIR, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  test("should merge secondary records into primary", async () => {
    const testPath = join(TEST_DIR, "wal-merge-test.raydb");
    cleanupTestFile(testPath);
    
    const db = await openGraphDB(testPath, {
      autoCheckpoint: false,
      walSize: 64 * 1024,
    });

    const pager = db._pager as FilePager;
    const walBuffer = createWalBuffer(pager, db._header!);

    // Write some records to primary
    walBuffer.writeRecord({ type: 1, txid: 1n, payload: new Uint8Array(8) });

    // Switch to secondary
    walBuffer.switchToSecondary();
    expect(walBuffer.getActiveRegion()).toBe(1);

    // Write to secondary
    walBuffer.writeRecord({ type: 1, txid: 2n, payload: new Uint8Array(8) });

    // Merge secondary into primary
    walBuffer.mergeSecondaryIntoPrimary();

    // Should be back in primary region
    expect(walBuffer.getActiveRegion()).toBe(0);

    // Primary should have records from both regions
    const records = walBuffer.scanRegion(0);
    expect(records.length).toBeGreaterThan(0);

    await closeGraphDB(db);
  });

  test("should track region usage correctly", async () => {
    const testPath = join(TEST_DIR, "wal-usage-test.raydb");
    cleanupTestFile(testPath);
    
    const db = await openGraphDB(testPath, {
      autoCheckpoint: false,
      walSize: 64 * 1024,
    });

    const pager = db._pager as FilePager;
    const walBuffer = createWalBuffer(pager, db._header!);

    // Initially no usage
    expect(walBuffer.getActiveRegionUsage()).toBe(0);
    expect(walBuffer.getSecondaryRegionUsage()).toBe(0);

    // Write some records
    walBuffer.writeRecord({ type: 1, txid: 1n, payload: new Uint8Array(100) });
    
    // Primary region should have some usage now
    expect(walBuffer.getActiveRegionUsage()).toBeGreaterThan(0);

    // Switch to secondary and write
    walBuffer.switchToSecondary();
    walBuffer.writeRecord({ type: 1, txid: 2n, payload: new Uint8Array(100) });

    // Secondary should have usage
    expect(walBuffer.getSecondaryRegionUsage()).toBeGreaterThan(0);

    await closeGraphDB(db);
  });
});
