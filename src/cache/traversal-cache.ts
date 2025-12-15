/**
 * Traversal Cache
 *
 * Caches neighbor iteration results to avoid repeated graph traversals.
 */

import type { ETypeID, NodeID } from "../types.ts";
import type { Edge } from "../types.ts";
import { LRUCache } from "../util/lru.ts";

type TraversalKey = string; // Format: `${NodeID}:${ETypeID | 'all'}:${'out' | 'in'}`

interface TraversalCacheConfig {
  maxEntries: number;
  maxNeighborsPerEntry: number;
}

interface CachedNeighbors {
  neighbors: Edge[];
  truncated: boolean; // True if neighbors were truncated due to maxNeighborsPerEntry
}

/**
 * Traversal cache for neighbor lookups
 */
export class TraversalCache {
  private readonly cache: LRUCache<TraversalKey, CachedNeighbors>;
  private readonly maxNeighborsPerEntry: number;
  private hits = 0;
  private misses = 0;

  constructor(config: TraversalCacheConfig) {
    this.cache = new LRUCache(config.maxEntries);
    this.maxNeighborsPerEntry = config.maxNeighborsPerEntry;
  }

  /**
   * Get cached neighbors for a node
   *
   * @param nodeId - Source node ID
   * @param etype - Edge type ID, or undefined for all types
   * @param direction - 'out' or 'in'
   * @returns Cached neighbors or undefined if not cached
   */
  get(
    nodeId: NodeID,
    etype: ETypeID | undefined,
    direction: "out" | "in",
  ): CachedNeighbors | undefined {
    const key = this.traversalKey(nodeId, etype, direction);
    const value = this.cache.get(key);
    if (value !== undefined) {
      this.hits++;
      return value;
    }
    this.misses++;
    return undefined;
  }

  /**
   * Set cached neighbors for a node
   *
   * @param nodeId - Source node ID
   * @param etype - Edge type ID, or undefined for all types
   * @param direction - 'out' or 'in'
   * @param neighbors - Array of neighbor edges
   */
  set(
    nodeId: NodeID,
    etype: ETypeID | undefined,
    direction: "out" | "in",
    neighbors: Edge[],
  ): void {
    const key = this.traversalKey(nodeId, etype, direction);
    
    // Truncate if exceeds max neighbors per entry
    let truncated = false;
    let cachedNeighbors = neighbors;
    if (neighbors.length > this.maxNeighborsPerEntry) {
      cachedNeighbors = neighbors.slice(0, this.maxNeighborsPerEntry);
      truncated = true;
    }

    this.cache.set(key, {
      neighbors: cachedNeighbors,
      truncated,
    });
  }

  /**
   * Invalidate all cached traversals for a node
   */
  invalidateNode(nodeId: NodeID): void {
    // Clear all entries for this node (both directions, all edge types)
    // Similar to property cache, we clear entire cache for simplicity
    // Could be optimized with node->keys mapping
    this.cache.clear();
  }

  /**
   * Invalidate traversals involving a specific edge
   */
  invalidateEdge(src: NodeID, etype: ETypeID, dst: NodeID): void {
    // Clear cache entries for both src (out) and dst (in) with this edge type
    // For simplicity, clear entire cache
    this.cache.clear();
  }

  /**
   * Clear all cached traversals
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

  /**
   * Generate cache key for traversal
   */
  private traversalKey(
    nodeId: NodeID,
    etype: ETypeID | undefined,
    direction: "out" | "in",
  ): TraversalKey {
    const etypeStr = etype === undefined ? "all" : String(etype);
    return `${nodeId}:${etypeStr}:${direction}`;
  }
}

