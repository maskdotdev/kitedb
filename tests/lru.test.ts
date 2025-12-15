/**
 * Tests for LRU Cache
 */

import { describe, expect, test } from "bun:test";
import { LRUCache } from "../src/util/lru.ts";

describe("LRUCache", () => {
  test("basic get and set", () => {
    const cache = new LRUCache<string, number>(10);
    expect(cache.get("a")).toBeUndefined();

    cache.set("a", 1);
    expect(cache.get("a")).toBe(1);
  });

  test("eviction when full", () => {
    const cache = new LRUCache<string, number>(3);
    cache.set("a", 1);
    cache.set("b", 2);
    cache.set("c", 3);
    expect(cache.size).toBe(3);

    // Adding a 4th item should evict "a" (least recently used)
    cache.set("d", 4);
    expect(cache.size).toBe(3);
    expect(cache.get("a")).toBeUndefined();
    expect(cache.get("b")).toBe(2);
    expect(cache.get("c")).toBe(3);
    expect(cache.get("d")).toBe(4);
  });

  test("get updates LRU order", () => {
    const cache = new LRUCache<string, number>(3);
    cache.set("a", 1);
    cache.set("b", 2);
    cache.set("c", 3);

    // Access "a" to make it most recently used
    cache.get("a");

    // Adding "d" should evict "b" (not "a")
    cache.set("d", 4);
    expect(cache.get("a")).toBe(1);
    expect(cache.get("b")).toBeUndefined();
    expect(cache.get("c")).toBe(3);
    expect(cache.get("d")).toBe(4);
  });

  test("update existing value", () => {
    const cache = new LRUCache<string, number>(10);
    cache.set("a", 1);
    cache.set("a", 2);
    expect(cache.get("a")).toBe(2);
    expect(cache.size).toBe(1);
  });

  test("delete", () => {
    const cache = new LRUCache<string, number>(10);
    cache.set("a", 1);
    cache.set("b", 2);

    expect(cache.delete("a")).toBe(true);
    expect(cache.get("a")).toBeUndefined();
    expect(cache.get("b")).toBe(2);
    expect(cache.size).toBe(1);

    expect(cache.delete("nonexistent")).toBe(false);
  });

  test("has", () => {
    const cache = new LRUCache<string, number>(10);
    expect(cache.has("a")).toBe(false);

    cache.set("a", 1);
    expect(cache.has("a")).toBe(true);
    expect(cache.has("b")).toBe(false);
  });

  test("clear", () => {
    const cache = new LRUCache<string, number>(10);
    cache.set("a", 1);
    cache.set("b", 2);
    expect(cache.size).toBe(2);

    cache.clear();
    expect(cache.size).toBe(0);
    expect(cache.get("a")).toBeUndefined();
    expect(cache.get("b")).toBeUndefined();
  });

  test("max size property", () => {
    const cache = new LRUCache<string, number>(42);
    expect(cache.max).toBe(42);
  });

  test("throws on invalid max size", () => {
    expect(() => new LRUCache<string, number>(0)).toThrow();
    expect(() => new LRUCache<string, number>(-1)).toThrow();
  });

  test("complex eviction scenario", () => {
    const cache = new LRUCache<string, number>(3);
    cache.set("a", 1);
    cache.set("b", 2);
    cache.set("c", 3);

    // Access "b" - makes it most recently used
    cache.get("b");

    // Add "d" - should evict "a" (least recently used, "c" was accessed before "b")
    cache.set("d", 4);
    expect(cache.get("a")).toBeUndefined();

    // Access "c" - makes it most recently used
    cache.get("c");

    // Add "e" - should evict "b" (least recently used now)
    cache.set("e", 5);
    expect(cache.get("b")).toBeUndefined();
    expect(cache.get("c")).toBe(3);
    expect(cache.get("d")).toBe(4);
    expect(cache.get("e")).toBe(5);
  });
});

