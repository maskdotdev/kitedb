/**
 * Tests for node and edge listing functionality
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { defineEdge, defineNode, ray, optional, prop } from "../src/index.ts";
import {
  listNodes,
  listEdges,
  countNodes,
  countEdges,
  openGraphDB,
  closeGraphDB,
  beginTx,
  commit,
  createNode,
  addEdge,
  deleteNode,
  defineEtype,
} from "../src/ray/graph-db/index.ts";

// ============================================================================
// Schema Definition
// ============================================================================

const user = defineNode("user", {
  key: (id: string) => `user:${id}`,
  props: {
    name: prop.string("name"),
    age: prop.int("age"),
  },
});

const post = defineNode("post", {
  key: (id: number) => `post:${id}`,
  props: {
    title: prop.string("title"),
    content: prop.string("content"),
  },
});

const follows = defineEdge("follows");

const likes = defineEdge("likes", {
  timestamp: prop.int("timestamp"),
});

// ============================================================================
// Low-Level API Tests
// ============================================================================

describe("Low-Level Listing API", () => {
  let testDir: string;
  let testPath: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-listing-test-"));
    testPath = join(testDir, "db.raydb");
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  describe("listNodes()", () => {
    test("empty database returns no nodes", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      
      const nodes = [...listNodes(db)];
      expect(nodes).toHaveLength(0);
      
      await closeGraphDB(db);
    });

    test("lists nodes created in transaction", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      const tx = beginTx(db);
      
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      const node3 = createNode(tx, { key: "node3" });
      
      await commit(tx);
      
      const nodes = [...listNodes(db)];
      expect(nodes).toHaveLength(3);
      expect(nodes).toContain(node1);
      expect(nodes).toContain(node2);
      expect(nodes).toContain(node3);
      
      await closeGraphDB(db);
    });

    test("excludes deleted nodes", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      
      // Create nodes
      const tx1 = beginTx(db);
      const node1 = createNode(tx1, { key: "node1" });
      const node2 = createNode(tx1, { key: "node2" });
      const node3 = createNode(tx1, { key: "node3" });
      await commit(tx1);
      
      // Delete one node
      const tx2 = beginTx(db);
      deleteNode(tx2, node2);
      await commit(tx2);
      
      const nodes = [...listNodes(db)];
      expect(nodes).toHaveLength(2);
      expect(nodes).toContain(node1);
      expect(nodes).not.toContain(node2);
      expect(nodes).toContain(node3);
      
      await closeGraphDB(db);
    });

    test("works with TxHandle for pending nodes", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      const tx = beginTx(db);
      
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      
      // List should include pending nodes from transaction
      const nodes = [...listNodes(tx)];
      expect(nodes).toHaveLength(2);
      expect(nodes).toContain(node1);
      expect(nodes).toContain(node2);
      
      await commit(tx);
      await closeGraphDB(db);
    });
  });

  describe("countNodes()", () => {
    test("empty database returns 0", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      
      expect(countNodes(db)).toBe(0);
      
      await closeGraphDB(db);
    });

    test("counts all nodes", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      const tx = beginTx(db);
      
      createNode(tx, { key: "node1" });
      createNode(tx, { key: "node2" });
      createNode(tx, { key: "node3" });
      createNode(tx, { key: "node4" });
      createNode(tx, { key: "node5" });
      
      await commit(tx);
      
      expect(countNodes(db)).toBe(5);
      
      await closeGraphDB(db);
    });

    test("accounts for deleted nodes", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      
      // Create nodes
      const tx1 = beginTx(db);
      createNode(tx1, { key: "node1" });
      const node2 = createNode(tx1, { key: "node2" });
      createNode(tx1, { key: "node3" });
      await commit(tx1);
      
      expect(countNodes(db)).toBe(3);
      
      // Delete one
      const tx2 = beginTx(db);
      deleteNode(tx2, node2);
      await commit(tx2);
      
      expect(countNodes(db)).toBe(2);
      
      await closeGraphDB(db);
    });

    test("counts pending transaction nodes", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      const tx = beginTx(db);
      
      createNode(tx, { key: "node1" });
      createNode(tx, { key: "node2" });
      
      // Count should include pending
      expect(countNodes(tx)).toBe(2);
      
      await commit(tx);
      await closeGraphDB(db);
    });
  });

  describe("listEdges()", () => {
    test("empty database returns no edges", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      
      const edges = [...listEdges(db)];
      expect(edges).toHaveLength(0);
      
      await closeGraphDB(db);
    });

    test("lists all edges", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      const tx = beginTx(db);
      
      const followsType = defineEtype(tx, "follows");
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      const node3 = createNode(tx, { key: "node3" });
      
      addEdge(tx, node1, followsType, node2);
      addEdge(tx, node1, followsType, node3);
      addEdge(tx, node2, followsType, node3);
      
      await commit(tx);
      
      const edges = [...listEdges(db)];
      expect(edges).toHaveLength(3);
      
      // Verify edge structure
      const edge1 = edges.find(e => e.src === node1 && e.dst === node2);
      expect(edge1).toBeDefined();
      expect(edge1?.etype).toBe(followsType);
      
      await closeGraphDB(db);
    });

    test("filters by edge type", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      const tx = beginTx(db);
      
      const followsType = defineEtype(tx, "follows");
      const likesType = defineEtype(tx, "likes");
      
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      const node3 = createNode(tx, { key: "node3" });
      
      addEdge(tx, node1, followsType, node2);
      addEdge(tx, node1, likesType, node3);
      addEdge(tx, node2, followsType, node3);
      
      await commit(tx);
      
      // All edges
      expect([...listEdges(db)]).toHaveLength(3);
      
      // Only follows
      const followsEdges = [...listEdges(db, { etype: followsType })];
      expect(followsEdges).toHaveLength(2);
      expect(followsEdges.every(e => e.etype === followsType)).toBe(true);
      
      // Only likes
      const likesEdges = [...listEdges(db, { etype: likesType })];
      expect(likesEdges).toHaveLength(1);
      expect(likesEdges[0].etype).toBe(likesType);
      
      await closeGraphDB(db);
    });
  });

  describe("countEdges()", () => {
    test("empty database returns 0", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      
      expect(countEdges(db)).toBe(0);
      
      await closeGraphDB(db);
    });

    test("counts all edges", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      const tx = beginTx(db);
      
      const followsType = defineEtype(tx, "follows");
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      const node3 = createNode(tx, { key: "node3" });
      
      addEdge(tx, node1, followsType, node2);
      addEdge(tx, node1, followsType, node3);
      addEdge(tx, node2, followsType, node3);
      
      await commit(tx);
      
      expect(countEdges(db)).toBe(3);
      
      await closeGraphDB(db);
    });

    test("counts filtered by edge type", async () => {
      const db = await openGraphDB(testPath, { createIfMissing: true });
      const tx = beginTx(db);
      
      const followsType = defineEtype(tx, "follows");
      const likesType = defineEtype(tx, "likes");
      
      const node1 = createNode(tx, { key: "node1" });
      const node2 = createNode(tx, { key: "node2" });
      
      addEdge(tx, node1, followsType, node2);
      addEdge(tx, node1, likesType, node2);
      addEdge(tx, node2, followsType, node1);
      
      await commit(tx);
      
      expect(countEdges(db)).toBe(3);
      expect(countEdges(db, { etype: followsType })).toBe(2);
      expect(countEdges(db, { etype: likesType })).toBe(1);
      
      await closeGraphDB(db);
    });
  });
});

// ============================================================================
// High-Level API Tests
// ============================================================================

describe("High-Level Listing API", () => {
  let testDir: string;
  let testPath: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-listing-hl-test-"));
    testPath = join(testDir, "db.raydb");
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  describe("Ray.all()", () => {
    test("lists all nodes of a type", async () => {
      const db = await ray(testPath, {
        nodes: [user, post],
        edges: [follows, likes],
      });

      // Create users
      await db.insert(user).values({ key: "alice", name: "Alice", age: 30n }).execute();
      await db.insert(user).values({ key: "bob", name: "Bob", age: 25n }).execute();
      await db.insert(user).values({ key: "charlie", name: "Charlie", age: 35n }).execute();
      
      // Create posts
      await db.insert(post).values({ key: 1, title: "Post 1", content: "Content 1" }).execute();
      await db.insert(post).values({ key: 2, title: "Post 2", content: "Content 2" }).execute();

      // List all users
      const users = [];
      for await (const u of db.all(user)) {
        users.push(u);
      }
      
      expect(users).toHaveLength(3);
      expect(users.map(u => u.name).sort()).toEqual(["Alice", "Bob", "Charlie"]);
      
      // Verify user properties
      const alice = users.find(u => u.name === "Alice");
      expect(alice?.$key).toBe("user:alice");
      expect(alice?.age).toBe(30n);

      // List all posts
      const posts = [];
      for await (const p of db.all(post)) {
        posts.push(p);
      }
      
      expect(posts).toHaveLength(2);
      expect(posts.map(p => p.title).sort()).toEqual(["Post 1", "Post 2"]);

      await db.close();
    });

    test("returns empty for no matching nodes", async () => {
      const db = await ray(testPath, {
        nodes: [user, post],
        edges: [follows],
      });

      // Create only posts
      await db.insert(post).values({ key: 1, title: "Post 1", content: "Content 1" }).execute();

      // List users (should be empty)
      const users = [];
      for await (const u of db.all(user)) {
        users.push(u);
      }
      
      expect(users).toHaveLength(0);

      await db.close();
    });
  });

  describe("Ray.count()", () => {
    test("counts all nodes without filter", async () => {
      const db = await ray(testPath, {
        nodes: [user, post],
        edges: [follows],
      });

      await db.insert(user).values({ key: "alice", name: "Alice", age: 30n }).execute();
      await db.insert(user).values({ key: "bob", name: "Bob", age: 25n }).execute();
      await db.insert(post).values({ key: 1, title: "Post 1", content: "Content 1" }).execute();

      // Count all nodes
      const total = await db.count();
      expect(total).toBe(3);

      await db.close();
    });

    test("counts nodes filtered by type", async () => {
      const db = await ray(testPath, {
        nodes: [user, post],
        edges: [follows],
      });

      await db.insert(user).values({ key: "alice", name: "Alice", age: 30n }).execute();
      await db.insert(user).values({ key: "bob", name: "Bob", age: 25n }).execute();
      await db.insert(post).values({ key: 1, title: "Post 1", content: "Content 1" }).execute();
      await db.insert(post).values({ key: 2, title: "Post 2", content: "Content 2" }).execute();
      await db.insert(post).values({ key: 3, title: "Post 3", content: "Content 3" }).execute();

      expect(await db.count()).toBe(5);
      expect(await db.count(user)).toBe(2);
      expect(await db.count(post)).toBe(3);

      await db.close();
    });
  });

  describe("Ray.allEdges()", () => {
    test("lists all edges", async () => {
      const db = await ray(testPath, {
        nodes: [user],
        edges: [follows, likes],
      });

      const alice = await db.insert(user).values({ key: "alice", name: "Alice", age: 30n }).returning();
      const bob = await db.insert(user).values({ key: "bob", name: "Bob", age: 25n }).returning();
      const charlie = await db.insert(user).values({ key: "charlie", name: "Charlie", age: 35n }).returning();

      await db.link(alice, follows, bob);
      await db.link(alice, follows, charlie);
      await db.link(bob, follows, charlie);

      const edges = [];
      for await (const edge of db.allEdges()) {
        edges.push(edge);
      }

      expect(edges).toHaveLength(3);

      await db.close();
    });

    test("filters edges by type", async () => {
      const db = await ray(testPath, {
        nodes: [user],
        edges: [follows, likes],
      });

      const alice = await db.insert(user).values({ key: "alice", name: "Alice", age: 30n }).returning();
      const bob = await db.insert(user).values({ key: "bob", name: "Bob", age: 25n }).returning();

      await db.link(alice, follows, bob);
      await db.link(bob, follows, alice);
      await db.link(alice, likes, bob, { timestamp: 1234567890n });

      // All edges
      const allEdges = [];
      for await (const edge of db.allEdges()) {
        allEdges.push(edge);
      }
      expect(allEdges).toHaveLength(3);

      // Only follows edges
      const followEdges = [];
      for await (const edge of db.allEdges(follows)) {
        followEdges.push(edge);
      }
      expect(followEdges).toHaveLength(2);

      // Only likes edges
      const likeEdges = [];
      for await (const edge of db.allEdges(likes)) {
        likeEdges.push(edge);
      }
      expect(likeEdges).toHaveLength(1);

      await db.close();
    });

    test("includes edge properties", async () => {
      const db = await ray(testPath, {
        nodes: [user],
        edges: [likes],
      });

      const alice = await db.insert(user).values({ key: "alice", name: "Alice", age: 30n }).returning();
      const bob = await db.insert(user).values({ key: "bob", name: "Bob", age: 25n }).returning();

      await db.link(alice, likes, bob, { timestamp: 1234567890n });

      const edges = [];
      for await (const edge of db.allEdges(likes)) {
        edges.push(edge);
      }

      expect(edges).toHaveLength(1);
      expect(edges[0].props.timestamp).toBe(1234567890n);

      await db.close();
    });
  });

  describe("Ray.countEdges()", () => {
    test("counts all edges", async () => {
      const db = await ray(testPath, {
        nodes: [user],
        edges: [follows, likes],
      });

      const alice = await db.insert(user).values({ key: "alice", name: "Alice", age: 30n }).returning();
      const bob = await db.insert(user).values({ key: "bob", name: "Bob", age: 25n }).returning();
      const charlie = await db.insert(user).values({ key: "charlie", name: "Charlie", age: 35n }).returning();

      await db.link(alice, follows, bob);
      await db.link(alice, follows, charlie);
      await db.link(bob, follows, charlie);
      await db.link(alice, likes, bob, { timestamp: 1n });

      expect(await db.countEdges()).toBe(4);

      await db.close();
    });

    test("counts edges filtered by type", async () => {
      const db = await ray(testPath, {
        nodes: [user],
        edges: [follows, likes],
      });

      const alice = await db.insert(user).values({ key: "alice", name: "Alice", age: 30n }).returning();
      const bob = await db.insert(user).values({ key: "bob", name: "Bob", age: 25n }).returning();

      await db.link(alice, follows, bob);
      await db.link(bob, follows, alice);
      await db.link(alice, likes, bob, { timestamp: 1n });
      await db.link(bob, likes, alice, { timestamp: 2n });

      expect(await db.countEdges()).toBe(4);
      expect(await db.countEdges(follows)).toBe(2);
      expect(await db.countEdges(likes)).toBe(2);

      await db.close();
    });
  });
});
