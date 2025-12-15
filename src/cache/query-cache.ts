/**
 * Query Cache
 *
 * Caches complex query results with optional TTL (time-to-live).
 * Uses content-addressed keys based on query parameters.
 */

import { LRUCache } from "../util/lru.ts";
import { xxhash64String } from "../util/hash.ts";

type QueryKey = string; // Hash of query parameters

interface QueryCacheConfig {
  maxEntries: number;
  ttlMs?: number; // Optional TTL in milliseconds
}

interface CachedQueryResult<T> {
  value: T;
  timestamp: number; // Unix timestamp in milliseconds
}

/**
 * Query cache for complex query results
 */
export class QueryCache {
  private readonly cache: LRUCache<QueryKey, CachedQueryResult<unknown>>;
  private readonly ttlMs: number | undefined;
  private hits = 0;
  private misses = 0;

  constructor(config: QueryCacheConfig) {
    this.cache = new LRUCache(config.maxEntries);
    this.ttlMs = config.ttlMs;
  }

  /**
   * Get a cached query result
   *
   * @param queryKey - Cache key (typically a hash of query parameters)
   * @returns Cached result or undefined if not cached or expired
   */
  get<T>(queryKey: QueryKey): T | undefined {
    const cached = this.cache.get(queryKey);
    if (!cached) {
      this.misses++;
      return undefined;
    }

    // Check TTL if configured
    if (this.ttlMs !== undefined) {
      const now = Date.now();
      const age = now - cached.timestamp;
      if (age > this.ttlMs) {
        // Expired, remove from cache
        this.cache.delete(queryKey);
        this.misses++;
        return undefined;
      }
    }

    this.hits++;
    return cached.value as T;
  }

  /**
   * Set a query result in cache
   *
   * @param queryKey - Cache key (typically a hash of query parameters)
   * @param value - Result value to cache
   */
  set<T>(queryKey: QueryKey, value: T): void {
    const cached: CachedQueryResult<T> = {
      value,
      timestamp: Date.now(),
    };
    this.cache.set(queryKey, cached as CachedQueryResult<unknown>);
  }

  /**
   * Generate a cache key from query parameters
   *
   * @param params - Query parameters (will be serialized and hashed)
   * @returns Cache key
   */
  static generateKey(params: Record<string, unknown> | string): QueryKey {
    if (typeof params === "string") {
      return params;
    }

    // Sort keys for consistent hashing
    const sorted = Object.keys(params)
      .sort()
      .map((key) => `${key}:${JSON.stringify(params[key])}`)
      .join("|");

    // Hash the serialized parameters
    return String(xxhash64String(sorted));
  }

  /**
   * Clear all cached queries
   */
  clear(): void {
    this.cache.clear();
    this.hits = 0;
    this.misses = 0;
  }

  /**
   * Get cache statistics
   */
  getStats() {
    const total = this.hits + this.misses;
    return {
      hits: this.hits,
      misses: this.misses,
      hitRate: total > 0 ? this.hits / total : 0,
      size: this.cache.size,
      maxSize: this.cache.max,
    };
  }
}

