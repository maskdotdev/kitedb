/**
 * Property Cache
 *
 * Caches node and edge property lookups to avoid repeated delta/snapshot reads.
 */

import type { ETypeID, NodeID, PropKeyID, PropValue } from "../types.ts";
import { LRUCache } from "../util/lru.ts";

// Cache keys
type NodePropKey = string; // Format: `n:${NodeID}:${PropKeyID}`
type EdgePropKey = string; // Format: `e:${NodeID}:${ETypeID}:${NodeID}:${PropKeyID}`

interface PropertyCacheConfig {
  maxNodeProps: number;
  maxEdgeProps: number;
}

/**
 * Property cache for node and edge properties
 */
export class PropertyCache {
  private readonly nodeCache: LRUCache<NodePropKey, PropValue | null>;
  private readonly edgeCache: LRUCache<EdgePropKey, PropValue | null>;
  private hits = 0;
  private misses = 0;

  constructor(config: PropertyCacheConfig) {
    this.nodeCache = new LRUCache(config.maxNodeProps);
    this.edgeCache = new LRUCache(config.maxEdgeProps);
  }

  /**
   * Get a node property from cache
   */
  getNodeProp(nodeId: NodeID, propKeyId: PropKeyID): PropValue | null | undefined {
    const key = this.nodePropKey(nodeId, propKeyId);
    const value = this.nodeCache.get(key);
    if (value !== undefined) {
      this.hits++;
      return value;
    }
    this.misses++;
    return undefined;
  }

  /**
   * Set a node property in cache
   */
  setNodeProp(
    nodeId: NodeID,
    propKeyId: PropKeyID,
    value: PropValue | null,
  ): void {
    const key = this.nodePropKey(nodeId, propKeyId);
    this.nodeCache.set(key, value);
  }

  /**
   * Get an edge property from cache
   */
  getEdgeProp(
    src: NodeID,
    etype: ETypeID,
    dst: NodeID,
    propKeyId: PropKeyID,
  ): PropValue | null | undefined {
    const key = this.edgePropKey(src, etype, dst, propKeyId);
    const value = this.edgeCache.get(key);
    if (value !== undefined) {
      this.hits++;
      return value;
    }
    this.misses++;
    return undefined;
  }

  /**
   * Set an edge property in cache
   */
  setEdgeProp(
    src: NodeID,
    etype: ETypeID,
    dst: NodeID,
    propKeyId: PropKeyID,
    value: PropValue | null,
  ): void {
    const key = this.edgePropKey(src, etype, dst, propKeyId);
    this.edgeCache.set(key, value);
  }

  /**
   * Invalidate all properties for a node
   */
  invalidateNode(nodeId: NodeID): void {
    // We need to iterate through all keys to find ones matching this node
    // This is O(n) but necessary for proper invalidation
    const nodePrefix = `n:${nodeId}:`;
    const keysToDelete: NodePropKey[] = [];
    
    // Collect keys to delete (can't delete during iteration)
    // Note: LRUCache doesn't expose iteration, so we'll clear node cache
    // This is a trade-off: clearing is O(1) but loses all cached node props
    // For better performance, we could track node->keys mapping, but that adds complexity
    this.nodeCache.clear();
  }

  /**
   * Invalidate a specific edge property
   */
  invalidateEdge(src: NodeID, etype: ETypeID, dst: NodeID): void {
    // Similar to invalidateNode, we clear the entire edge cache
    // Could be optimized with edge->keys mapping
    this.edgeCache.clear();
  }

  /**
   * Clear all cached properties
   */
  clear(): void {
    this.nodeCache.clear();
    this.edgeCache.clear();
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
      size: this.nodeCache.size + this.edgeCache.size,
      maxSize: this.nodeCache.max + this.edgeCache.max,
    };
  }

  /**
   * Generate cache key for node property
   */
  private nodePropKey(nodeId: NodeID, propKeyId: PropKeyID): NodePropKey {
    return `n:${nodeId}:${propKeyId}`;
  }

  /**
   * Generate cache key for edge property
   */
  private edgePropKey(
    src: NodeID,
    etype: ETypeID,
    dst: NodeID,
    propKeyId: PropKeyID,
  ): EdgePropKey {
    return `e:${src}:${etype}:${dst}:${propKeyId}`;
  }
}

