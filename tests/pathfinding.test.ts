/**
 * Tests for pathfinding algorithms (Dijkstra, A*)
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { defineEdge, defineNode, ray, prop } from "../src/index.ts";

// ============================================================================
// Schema Definition
// ============================================================================

const city = defineNode("city", {
  key: (id: string) => `city:${id}`,
  props: {
    name: prop.string("name"),
    lat: prop.float("lat"),
    lon: prop.float("lon"),
  },
});

const road = defineEdge("road", {
  distance: prop.float("distance"),
  speedLimit: prop.int("speedLimit"),
});

const knows = defineEdge("knows", {
  strength: prop.float("strength"),
});

// ============================================================================
// Tests
// ============================================================================

describe("Path Finding", () => {
  let testDir: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-pathfinding-test-"));
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  describe("Dijkstra's Algorithm", () => {
    test("find shortest path in simple graph", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [road],
      });

      const a = await db
        .insert(city)
        .values({ key: "a", name: "City A", lat: 0.0, lon: 0.0 })
        .returning();

      const b = await db
        .insert(city)
        .values({ key: "b", name: "City B", lat: 1.0, lon: 0.0 })
        .returning();

      const c = await db
        .insert(city)
        .values({ key: "c", name: "City C", lat: 2.0, lon: 0.0 })
        .returning();

      // A -> B (distance 10)
      await db.link(a, road, b, { distance: 10.0 });
      // B -> C (distance 5)
      await db.link(b, road, c, { distance: 5.0 });
      // A -> C (distance 20, longer path)
      await db.link(a, road, c, { distance: 20.0 });

      const path = await db
        .shortestPath(a, { property: "distance" })
        .via(road)
        .to(c)
        .dijkstra();

      expect(path.found).toBe(true);
      expect(path.path).toHaveLength(3);
      expect(path.path[0]?.$id).toBe(a.$id);
      expect(path.path[1]?.$id).toBe(b.$id);
      expect(path.path[2]?.$id).toBe(c.$id);
      expect(path.totalWeight).toBe(15.0); // 10 + 5

      await db.close();
    });

    test("no path exists", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [road],
      });

      const a = await db
        .insert(city)
        .values({ key: "a", name: "City A", lat: 0.0, lon: 0.0 })
        .returning();

      const b = await db
        .insert(city)
        .values({ key: "b", name: "City B", lat: 1.0, lon: 0.0 })
        .returning();

      const path = await db
        .shortestPath(a, { property: "distance" })
        .via(road)
        .to(b)
        .dijkstra();

      expect(path.found).toBe(false);
      expect(path.path).toHaveLength(0);
      expect(path.totalWeight).toBe(Infinity);

      await db.close();
    });

    test("unweighted path (default weight)", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [road],
      });

      const a = await db
        .insert(city)
        .values({ key: "a", name: "City A", lat: 0.0, lon: 0.0 })
        .returning();

      const b = await db
        .insert(city)
        .values({ key: "b", name: "City B", lat: 1.0, lon: 0.0 })
        .returning();

      const c = await db
        .insert(city)
        .values({ key: "c", name: "City C", lat: 2.0, lon: 0.0 })
        .returning();

      // A -> B
      await db.link(a, road, b, { distance: 10.0 });
      // B -> C
      await db.link(b, road, c, { distance: 5.0 });
      // A -> C (direct)
      await db.link(a, road, c, { distance: 20.0 });

      // Unweighted: should find shortest hop count (A -> C)
      const path = await db.shortestPath(a).via(road).to(c).dijkstra();

      expect(path.found).toBe(true);
      expect(path.path.length).toBeLessThanOrEqual(3);
      expect(path.totalWeight).toBeGreaterThan(0);

      await db.close();
    });

    test("custom weight function", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [knows],
      });

      const a = await db
        .insert(city)
        .values({ key: "a", name: "Person A", lat: 0.0, lon: 0.0 })
        .returning();

      const b = await db
        .insert(city)
        .values({ key: "b", name: "Person B", lat: 1.0, lon: 0.0 })
        .returning();

      const c = await db
        .insert(city)
        .values({ key: "c", name: "Person C", lat: 2.0, lon: 0.0 })
        .returning();

      // A -> B (strength 0.5, weight = 1/0.5 = 2)
      await db.link(a, knows, b, { strength: 0.5 });
      // B -> C (strength 0.8, weight = 1/0.8 = 1.25)
      await db.link(b, knows, c, { strength: 0.8 });
      // A -> C (strength 0.3, weight = 1/0.3 = 3.33)
      await db.link(a, knows, c, { strength: 0.3 });

      // Use inverse strength as weight (higher strength = lower weight)
      const path = await db
        .shortestPath(a, { fn: (e) => 1 / (e.strength as number) })
        .via(knows)
        .to(c)
        .dijkstra();

      expect(path.found).toBe(true);
      // Should prefer A -> B -> C (2 + 1.25 = 3.25) over A -> C (3.33)
      expect(path.path.length).toBe(3);
      expect(path.path[0]?.$id).toBe(a.$id);
      expect(path.path[1]?.$id).toBe(b.$id);
      expect(path.path[2]?.$id).toBe(c.$id);

      await db.close();
    });

    test("maxDepth limits search", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [road],
      });

      const nodes = [];
      for (let i = 0; i < 5; i++) {
        const node = await db
          .insert(city)
          .values({
            key: `city${i}`,
            name: `City ${i}`,
            lat: i * 1.0,
            lon: 0.0,
          })
          .returning();
        nodes.push(node);
      }

      // Create chain: 0 -> 1 -> 2 -> 3 -> 4
      for (let i = 0; i < 4; i++) {
        await db.link(nodes[i]!, road, nodes[i + 1]!, { distance: 1.0 });
      }

      // With maxDepth 2, should not find path from 0 to 4
      const path = await db
        .shortestPath(nodes[0]!, { property: "distance" })
        .via(road)
        .maxDepth(2)
        .to(nodes[4]!)
        .dijkstra();

      expect(path.found).toBe(false);

      // With maxDepth 5, should find path
      const path2 = await db
        .shortestPath(nodes[0]!, { property: "distance" })
        .via(road)
        .maxDepth(5)
        .to(nodes[4]!)
        .dijkstra();

      expect(path2.found).toBe(true);
      expect(path2.path.length).toBe(5);

      await db.close();
    });

    test("direction: in vs out", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [road],
      });

      const a = await db
        .insert(city)
        .values({ key: "a", name: "City A", lat: 0.0, lon: 0.0 })
        .returning();

      const b = await db
        .insert(city)
        .values({ key: "b", name: "City B", lat: 1.0, lon: 0.0 })
        .returning();

      // A -> B
      await db.link(a, road, b, { distance: 10.0 });

      // Out direction: should find path
      const pathOut = await db
        .shortestPath(a, { property: "distance" })
        .via(road)
        .direction("out")
        .to(b)
        .dijkstra();

      expect(pathOut.found).toBe(true);

      // In direction: should not find path (B -> A doesn't exist)
      const pathIn = await db
        .shortestPath(a, { property: "distance" })
        .via(road)
        .direction("in")
        .to(b)
        .dijkstra();

      expect(pathIn.found).toBe(false);

      await db.close();
    });

    test("toAny finds path to any target", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [road],
      });

      const a = await db
        .insert(city)
        .values({ key: "a", name: "City A", lat: 0.0, lon: 0.0 })
        .returning();

      const b = await db
        .insert(city)
        .values({ key: "b", name: "City B", lat: 1.0, lon: 0.0 })
        .returning();

      const c = await db
        .insert(city)
        .values({ key: "c", name: "City C", lat: 2.0, lon: 0.0 })
        .returning();

      // A -> B
      await db.link(a, road, b, { distance: 10.0 });
      // A -> C
      await db.link(a, road, c, { distance: 5.0 });

      // Find path to any of [b, c] - should find C (shorter)
      const path = await db
        .shortestPath(a, { property: "distance" })
        .via(road)
        .toAny([b, c])
        .dijkstra();

      expect(path.found).toBe(true);
      expect(path.path[path.path.length - 1]?.$id).toBe(c.$id);
      expect(path.totalWeight).toBe(5.0);

      await db.close();
    });
  });

  describe("A* Algorithm", () => {
    test("find path with heuristic", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [road],
      });

      const a = await db
        .insert(city)
        .values({ key: "a", name: "City A", lat: 0.0, lon: 0.0 })
        .returning();

      const b = await db
        .insert(city)
        .values({ key: "b", name: "City B", lat: 1.0, lon: 0.0 })
        .returning();

      const c = await db
        .insert(city)
        .values({ key: "c", name: "City C", lat: 2.0, lon: 0.0 })
        .returning();

      // A -> B (distance 10)
      await db.link(a, road, b, { distance: 10.0 });
      // B -> C (distance 5)
      await db.link(b, road, c, { distance: 5.0 });
      // A -> C (distance 20)
      await db.link(a, road, c, { distance: 20.0 });

      // Simple Euclidean distance heuristic
      const heuristic = (current: typeof a, goal: typeof c) => {
        const dx = (current.lon as number) - (goal.lon as number);
        const dy = (current.lat as number) - (goal.lat as number);
        return Math.sqrt(dx * dx + dy * dy);
      };

      const path = await db
        .shortestPath(a, { property: "distance" })
        .via(road)
        .to(c)
        .aStar(heuristic);

      expect(path.found).toBe(true);
      expect(path.path.length).toBeGreaterThan(0);
      expect(path.totalWeight).toBeGreaterThan(0);

      await db.close();
    });

    test("A* finds optimal path", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [road],
      });

      const a = await db
        .insert(city)
        .values({ key: "a", name: "City A", lat: 0.0, lon: 0.0 })
        .returning();

      const b = await db
        .insert(city)
        .values({ key: "b", name: "City B", lat: 1.0, lon: 1.0 })
        .returning();

      const c = await db
        .insert(city)
        .values({ key: "c", name: "City C", lat: 2.0, lon: 0.0 })
        .returning();

      // A -> B (distance 10)
      await db.link(a, road, b, { distance: 10.0 });
      // B -> C (distance 5)
      await db.link(b, road, c, { distance: 5.0 });
      // A -> C (distance 20)
      await db.link(a, road, c, { distance: 20.0 });

      // Manhattan distance heuristic
      const heuristic = (current: typeof a, goal: typeof c) => {
        const dx = Math.abs((current.lon as number) - (goal.lon as number));
        const dy = Math.abs((current.lat as number) - (goal.lat as number));
        return dx + dy;
      };

      const path = await db
        .shortestPath(a, { property: "distance" })
        .via(road)
        .to(c)
        .aStar(heuristic);

      expect(path.found).toBe(true);
      // Should find A -> B -> C (15) not A -> C (20)
      expect(path.totalWeight).toBe(15.0);

      await db.close();
    });
  });

  describe("Edge Cases", () => {
    test("source equals target", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [road],
      });

      const a = await db
        .insert(city)
        .values({ key: "a", name: "City A", lat: 0.0, lon: 0.0 })
        .returning();

      const path = await db
        .shortestPath(a, { property: "distance" })
        .via(road)
        .to(a)
        .dijkstra();

      // Path from node to itself should be found (trivial path)
      expect(path.found).toBe(true);
      expect(path.path.length).toBeGreaterThanOrEqual(1);

      await db.close();
    });

    test("cycle in graph", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [road],
      });

      const a = await db
        .insert(city)
        .values({ key: "a", name: "City A", lat: 0.0, lon: 0.0 })
        .returning();

      const b = await db
        .insert(city)
        .values({ key: "b", name: "City B", lat: 1.0, lon: 0.0 })
        .returning();

      // Create cycle: A -> B -> A
      await db.link(a, road, b, { distance: 10.0 });
      await db.link(b, road, a, { distance: 10.0 });

      const path = await db
        .shortestPath(a, { property: "distance" })
        .via(road)
        .to(b)
        .dijkstra();

      expect(path.found).toBe(true);
      expect(path.path.length).toBe(2); // Should not get stuck in cycle

      await db.close();
    });

    test("multiple edge types", async () => {
      const db = await ray(testDir, {
        nodes: [city],
        edges: [road, knows],
      });

      const a = await db
        .insert(city)
        .values({ key: "a", name: "Person A", lat: 0.0, lon: 0.0 })
        .returning();

      const b = await db
        .insert(city)
        .values({ key: "b", name: "Person B", lat: 1.0, lon: 0.0 })
        .returning();

      // A -> B via road
      await db.link(a, road, b, { distance: 10.0 });
      // A -> B via knows
      await db.link(a, knows, b, { strength: 0.5 });

      // Only traverse via road
      const path = await db
        .shortestPath(a, { property: "distance" })
        .via(road)
        .to(b)
        .dijkstra();

      expect(path.found).toBe(true);
      expect(path.edges.length).toBe(1);
      expect(path.edges[0]?.$etype).toBeDefined();

      await db.close();
    });
  });
});
