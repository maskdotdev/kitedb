/**
 * Tests for caching layer integration
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  addEdge,
  beginTx,
  clearCache,
  commit,
  createNode,
  defineEtype,
  definePropkey,
  getCacheStats,
  getEdgeProp,
  getNeighborsIn,
  getNeighborsOut,
  getNodeProp,
  invalidateEdgeCache,
  invalidateNodeCache,
  openGraphDB,
  setEdgeProp,
  setNodeProp,
} from "../src/index.ts";
import type { GraphDB, NodeID } from "../src/types.ts";

describe("Cache Integration", () => {
  let testDir: string;
  let testPath: string;
  let db: GraphDB;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-cache-test-"));
    testPath = join(testDir, "db.raydb");
    db = await openGraphDB(testPath, {
      cache: {
        enabled: true,
        propertyCache: { maxNodeProps: 100, maxEdgeProps: 100 },
        traversalCache: { maxEntries: 50, maxNeighborsPerEntry: 10 },
        queryCache: { maxEntries: 20 },
      },
    });
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  describe("Property Cache", () => {
    test("caches node properties", async () => {
      const tx = beginTx(db);
      const propKeyId = definePropkey(tx, "name");
      const nodeId = createNode(tx, { key: "test-node" });
      setNodeProp(tx, nodeId, propKeyId, { tag: 4, value: "Alice" });
      await commit(tx);

      // First access - cache miss
      const stats1 = getCacheStats(db);
      const value1 = getNodeProp(db, nodeId, propKeyId);
      expect(value1).not.toBeNull();
      const stats2 = getCacheStats(db);
      expect(stats2!.propertyCache.misses).toBeGreaterThan(
        stats1?.propertyCache.misses || 0,
      );

      // Second access - cache hit
      const stats3 = getCacheStats(db);
      const value2 = getNodeProp(db, nodeId, propKeyId);
      expect(value2).toEqual(value1);
      const stats4 = getCacheStats(db);
      expect(stats4!.propertyCache.hits).toBeGreaterThan(
        stats3?.propertyCache.hits || 0,
      );
    });

    test("caches edge properties", async () => {
      const tx = beginTx(db);
      const propKeyId = definePropkey(tx, "weight");
      const etypeId = defineEtype(tx, "knows");
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      addEdge(tx, node1, etypeId, node2);
      setEdgeProp(tx, node1, etypeId, node2, propKeyId, { tag: 3, value: 0.5 });
      await commit(tx);

      // First access - cache miss
      const stats1 = getCacheStats(db);
      const value1 = getEdgeProp(db, node1, etypeId, node2, propKeyId);
      expect(value1).not.toBeNull();
      const stats2 = getCacheStats(db);
      expect(stats2!.propertyCache.misses).toBeGreaterThan(
        stats1?.propertyCache.misses || 0,
      );

      // Second access - cache hit
      const stats3 = getCacheStats(db);
      const value2 = getEdgeProp(db, node1, etypeId, node2, propKeyId);
      expect(value2).toEqual(value1);
      const stats4 = getCacheStats(db);
      expect(stats4!.propertyCache.hits).toBeGreaterThan(
        stats3?.propertyCache.hits || 0,
      );
    });

    test("write-through invalidation on property update", async () => {
      const tx = beginTx(db);
      const propKeyId = definePropkey(tx, "name");
      const nodeId = createNode(tx, { key: "test-node" });
      setNodeProp(tx, nodeId, propKeyId, { tag: 4, value: "Alice" });
      await commit(tx);

      // Cache the value
      getNodeProp(db, nodeId, propKeyId);
      const stats1 = getCacheStats(db);
      const hitCount1 = stats1!.propertyCache.hits;

      // Update property - should invalidate cache
      const tx2 = beginTx(db);
      setNodeProp(tx2, nodeId, propKeyId, { tag: 4, value: "Bob" });
      await commit(tx2);

      // Next read should be a miss (cache was invalidated)
      const value = getNodeProp(db, nodeId, propKeyId);
      expect(value?.value).toBe("Bob");
    });
  });

  describe("Traversal Cache", () => {
    test("caches neighbor traversals", async () => {
      const tx = beginTx(db);
      const etypeId = defineEtype(tx, "knows");
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      const node3 = createNode(tx, { key: "node3" });
      addEdge(tx, node1, etypeId, node2);
      addEdge(tx, node1, etypeId, node3);
      await commit(tx);

      // First traversal - cache miss
      const stats1 = getCacheStats(db);
      const neighbors1 = Array.from(getNeighborsOut(db, node1, etypeId));
      expect(neighbors1).toHaveLength(2);
      const stats2 = getCacheStats(db);
      expect(stats2!.traversalCache.misses).toBeGreaterThan(
        stats1?.traversalCache.misses || 0,
      );

      // Second traversal - cache hit
      const stats3 = getCacheStats(db);
      const neighbors2 = Array.from(getNeighborsOut(db, node1, etypeId));
      expect(neighbors2).toHaveLength(2);
      const stats4 = getCacheStats(db);
      expect(stats4!.traversalCache.hits).toBeGreaterThan(
        stats3?.traversalCache.hits || 0,
      );
    });

    test("write-through invalidation on edge add", async () => {
      const tx = beginTx(db);
      const etypeId = defineEtype(tx, "knows");
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      addEdge(tx, node1, etypeId, node2);
      await commit(tx);

      // Cache the traversal
      Array.from(getNeighborsOut(db, node1, etypeId));
      const stats1 = getCacheStats(db);
      const hitCount1 = stats1!.traversalCache.hits;

      // Add another edge - should invalidate cache
      const tx2 = beginTx(db);
      const node3 = createNode(tx2, { key: "node3" });
      addEdge(tx2, node1, etypeId, node3);
      await commit(tx2);

      // Next traversal should be a miss
      const neighbors = Array.from(getNeighborsOut(db, node1, etypeId));
      expect(neighbors).toHaveLength(2);
    });
  });

  describe("Manual Invalidation", () => {
    test("invalidateNodeCache clears node-related caches", async () => {
      const tx = beginTx(db);
      const propKeyId = definePropkey(tx, "name");
      const etypeId = defineEtype(tx, "knows");
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      setNodeProp(tx, node1, propKeyId, { tag: 4, value: "Alice" });
      addEdge(tx, node1, etypeId, node2);
      await commit(tx);

      // Cache some data
      getNodeProp(db, node1, propKeyId);
      Array.from(getNeighborsOut(db, node1, etypeId));

      // Manual invalidation
      invalidateNodeCache(db, node1);

      // Next reads should be misses
      const stats1 = getCacheStats(db);
      getNodeProp(db, node1, propKeyId);
      Array.from(getNeighborsOut(db, node1, etypeId));
      const stats2 = getCacheStats(db);
      expect(stats2!.propertyCache.misses).toBeGreaterThan(
        stats1?.propertyCache.misses || 0,
      );
    });

    test("invalidateEdgeCache clears edge-related caches", async () => {
      const tx = beginTx(db);
      const propKeyId = definePropkey(tx, "weight");
      const etypeId = defineEtype(tx, "knows");
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      addEdge(tx, node1, etypeId, node2);
      setEdgeProp(tx, node1, etypeId, node2, propKeyId, { tag: 3, value: 0.5 });
      await commit(tx);

      // Cache some data
      getEdgeProp(db, node1, etypeId, node2, propKeyId);

      // Manual invalidation
      invalidateEdgeCache(db, node1, etypeId, node2);

      // Next read should be a miss
      const stats1 = getCacheStats(db);
      getEdgeProp(db, node1, etypeId, node2, propKeyId);
      const stats2 = getCacheStats(db);
      expect(stats2!.propertyCache.misses).toBeGreaterThan(
        stats1?.propertyCache.misses || 0,
      );
    });

    test("clearCache clears all caches", async () => {
      const tx = beginTx(db);
      const propKeyId = definePropkey(tx, "name");
      const etypeId = defineEtype(tx, "knows");
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      setNodeProp(tx, node1, propKeyId, { tag: 4, value: "Alice" });
      addEdge(tx, node1, etypeId, node2);
      await commit(tx);

      // Cache some data
      getNodeProp(db, node1, propKeyId);
      Array.from(getNeighborsOut(db, node1, etypeId));

      const stats1 = getCacheStats(db);
      expect(stats1!.propertyCache.size).toBeGreaterThan(0);

      // Clear all caches
      clearCache(db);

      const stats2 = getCacheStats(db);
      expect(stats2!.propertyCache.size).toBe(0);
      expect(stats2!.traversalCache.size).toBe(0);
    });
  });

  describe("Transaction-Aware Invalidation", () => {
    test("commit invalidates affected caches", async () => {
      const tx = beginTx(db);
      const propKeyId = definePropkey(tx, "name");
      const nodeId = createNode(tx, { key: "test-node" });
      setNodeProp(tx, nodeId, propKeyId, { tag: 4, value: "Alice" });
      await commit(tx);

      // Cache the value
      getNodeProp(db, nodeId, propKeyId);
      const stats1 = getCacheStats(db);
      const hitCount1 = stats1!.propertyCache.hits;

      // Update in transaction - commit should invalidate
      const tx2 = beginTx(db);
      setNodeProp(tx2, nodeId, propKeyId, { tag: 4, value: "Bob" });
      await commit(tx2);

      // Next read should be a miss
      const value = getNodeProp(db, nodeId, propKeyId);
      expect(value?.value).toBe("Bob");
    });
  });

  describe("Cache Statistics", () => {
    test("getCacheStats returns statistics", async () => {
      const tx = beginTx(db);
      const propKeyId = definePropkey(tx, "name");
      const nodeId = createNode(tx, { key: "test-node" });
      setNodeProp(tx, nodeId, propKeyId, { tag: 4, value: "Alice" });
      await commit(tx);

      const stats = getCacheStats(db);
      expect(stats).not.toBeNull();
      expect(stats!.propertyCache).toBeDefined();
      expect(stats!.traversalCache).toBeDefined();
      expect(stats!.queryCache).toBeDefined();
      expect(stats!.propertyCache.hits).toBeGreaterThanOrEqual(0);
      expect(stats!.propertyCache.misses).toBeGreaterThanOrEqual(0);
    });

    test("getCacheStats returns null when cache disabled", async () => {
      const dbNoCache = await openGraphDB(testPath + "-nocache", {
        cache: { enabled: false },
      });
      const stats = getCacheStats(dbNoCache);
      expect(stats).toBeNull();
    });
  });
});
