import { test, expect } from "bun:test";
import { nextPage, prevPage } from "../src/db/paging.ts";

test("nextPage returns same cursor when no next", () => {
  const meta = { cursor: undefined, nextCursor: undefined, hasMore: false };
  const result = nextPage(meta, []);
  expect(result.cursor).toBeUndefined();
  expect(result.history).toEqual([]);
});

test("nextPage advances and stores history", () => {
  const meta = { cursor: "c0", nextCursor: "c1", hasMore: true };
  const result = nextPage(meta, []);
  expect(result.cursor).toBe("c1");
  expect(result.history).toEqual(["c0"]);
});

test("prevPage pops history", () => {
  const result = prevPage(["c0", "c1"]);
  expect(result.cursor).toBe("c1");
  expect(result.history).toEqual(["c0"]);
});
