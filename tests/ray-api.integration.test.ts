import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { defineEdge, defineNode, prop, ray } from "../src/index.ts";

/**
 * Integration tests for the high-level `ray` API that
 * verify schema, property keys, and data persist correctly
 * across database close/reopen cycles.
 */

describe("Ray API Integration - Persistence", () => {
  let testDir: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-api-integration-"));
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  test("persists file node and properties across reopen", async () => {
    const fileNode = defineNode("file", {
      key: (path: string) => `file:${path}`,
      props: {
        filePath: prop.string("filePath"),
        hash: prop.string("hash"),
        language: prop.string("language"),
      },
    });

    // First open: create DB and insert a file node
    const db1 = await ray(testDir, {
      nodes: [fileNode],
      edges: [],
    });

    const created = await db1
      .insert(fileNode)
      .values({
        key: "/project/persistent.ts",
        filePath: "/project/persistent.ts",
        hash: "persistent-hash",
        language: "typescript",
      })
      .returning();

    const id1 = created.$id;

    // Sanity check before close
    const foundBefore = await db1.get(fileNode, "/project/persistent.ts");
    expect(foundBefore).not.toBeNull();
    expect(foundBefore!.filePath).toBe("/project/persistent.ts");
    expect(foundBefore!.hash).toBe("persistent-hash");
    expect(foundBefore!.language).toBe("typescript");

    await db1.close();

    // Second open: same schema, read back by key
    const db2 = await ray(testDir, {
      nodes: [fileNode],
      edges: [],
    });

    const foundAfter = await db2.get(fileNode, "/project/persistent.ts");

    expect(foundAfter).not.toBeNull();
    expect(foundAfter!.$id).toBe(id1);
    expect(foundAfter!.filePath).toBe("/project/persistent.ts");
    expect(foundAfter!.hash).toBe("persistent-hash");
    expect(foundAfter!.language).toBe("typescript");

    await db2.close();
  });

  test("persists multiple node types and edge properties across reopen", async () => {
    const fileNode = defineNode("file", {
      key: (path: string) => `file:${path}`,
      props: {
        filePath: prop.string("filePath"),
        hash: prop.string("hash"),
      },
    });

    const symbolNode = defineNode("symbol", {
      key: (id: string) => `symbol:${id}`,
      props: {
        name: prop.string("name"),
        kind: prop.string("kind"),
        filePath: prop.string("filePath"),
      },
    });

    const callsEdge = defineEdge("CALLS", {
      kind: prop.string("kind"),
    });

    // First open: create DB, insert file + symbol nodes, and a CALLS edge
    const db1 = await ray(testDir, {
      nodes: [fileNode, symbolNode],
      edges: [callsEdge],
    });

    const file = await db1
      .insert(fileNode)
      .values({
        key: "/project/src/math.ts",
        filePath: "/project/src/math.ts",
        hash: "h1",
      })
      .returning();

    const caller = await db1
      .insert(symbolNode)
      .values({
        key: "function:/project/src/math.ts:main:1",
        name: "main",
        kind: "function",
        filePath: file.filePath,
      })
      .returning();

    const callee = await db1
      .insert(symbolNode)
      .values({
        key: "function:/project/src/math.ts:helper:10",
        name: "helper",
        kind: "function",
        filePath: file.filePath,
      })
      .returning();

    await db1.link(caller, callsEdge, callee, { kind: "call" });

    await db1.close();

    // Second open: same schema, verify nodes and edge still exist
    const db2 = await ray(testDir, {
      nodes: [fileNode, symbolNode],
      edges: [callsEdge],
    });

    const reopenedFile = await db2.get(fileNode, "/project/src/math.ts");
    expect(reopenedFile).not.toBeNull();
    expect(reopenedFile!.hash).toBe("h1");

    const reopenedCaller = await db2.get(
      symbolNode,
      "function:/project/src/math.ts:main:1",
    );
    const reopenedCallee = await db2.get(
      symbolNode,
      "function:/project/src/math.ts:helper:10",
    );

    expect(reopenedCaller).not.toBeNull();
    expect(reopenedCaller!.name).toBe("main");
    expect(reopenedCallee).not.toBeNull();
    expect(reopenedCallee!.name).toBe("helper");

    // Verify CALLS edge still exists via traversal
    const neighbors = await db2.from(reopenedCaller!).out(callsEdge).toArray();
    expect(neighbors.length).toBeGreaterThanOrEqual(1);
    expect(neighbors[0]!.name).toBe("helper");

    await db2.close();
  });

  test("reopening multiple times keeps property key alignment", async () => {
    const fileNode = defineNode("file", {
      key: (path: string) => `file:${path}`,
      props: {
        filePath: prop.string("filePath"),
        hash: prop.string("hash"),
      },
    });

    // First open: create and insert
    let db = await ray(testDir, {
      nodes: [fileNode],
      edges: [],
    });

    await db
      .insert(fileNode)
      .values({
        key: "/project/reopen.ts",
        filePath: "/project/reopen.ts",
        hash: "v1",
      })
      .execute();

    await db.close();

    // Second open: update hash
    db = await ray(testDir, {
      nodes: [fileNode],
      edges: [],
    });

    await db
      .update(fileNode)
      .set({ hash: "v2" })
      .where({ $key: "file:/project/reopen.ts" })
      .execute();

    await db.close();

    // Third open: verify latest value is visible
    db = await ray(testDir, {
      nodes: [fileNode],
      edges: [],
    });

    const reopened = await db.get(fileNode, "/project/reopen.ts");
    expect(reopened).not.toBeNull();
    expect(reopened!.filePath).toBe("/project/reopen.ts");
    expect(reopened!.hash).toBe("v2");

    await db.close();
  });
});
