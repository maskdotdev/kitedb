/**
 * LRU (Least Recently Used) Cache
 *
 * Generic LRU cache implementation with O(1) get/set/delete operations.
 * Uses a Map for O(1) lookups and a doubly-linked list for O(1) eviction.
 */

interface LRUNode<K, V> {
  key: K;
  value: V;
  prev: LRUNode<K, V> | null;
  next: LRUNode<K, V> | null;
}

/**
 * LRU Cache implementation
 *
 * Maintains items in order of access, automatically evicting the least
 * recently used item when capacity is exceeded.
 */
export class LRUCache<K, V> {
  private readonly maxSize: number;
  private readonly cache: Map<K, LRUNode<K, V>>;
  private head: LRUNode<K, V> | null = null;
  private tail: LRUNode<K, V> | null = null;

  /**
   * Create a new LRU cache
   *
   * @param maxSize - Maximum number of items to cache
   */
  constructor(maxSize: number) {
    if (maxSize <= 0) {
      throw new Error("LRU cache maxSize must be greater than 0");
    }
    this.maxSize = maxSize;
    this.cache = new Map();
  }

  /**
   * Get a value from the cache
   * O(1) time complexity
   *
   * @param key - Cache key
   * @returns Cached value or undefined if not found
   */
  get(key: K): V | undefined {
    const node = this.cache.get(key);
    if (!node) {
      return undefined;
    }

    // Move to front (most recently used)
    this.moveToFront(node);
    return node.value;
  }

  /**
   * Set a value in the cache
   * O(1) time complexity
   *
   * @param key - Cache key
   * @param value - Value to cache
   */
  set(key: K, value: V): void {
    const existing = this.cache.get(key);
    if (existing) {
      // Update existing value and move to front
      existing.value = value;
      this.moveToFront(existing);
      return;
    }

    // Create new node
    const node: LRUNode<K, V> = {
      key,
      value,
      prev: null,
      next: null,
    };

    // Add to front
    if (!this.head) {
      this.head = node;
      this.tail = node;
    } else {
      node.next = this.head;
      this.head.prev = node;
      this.head = node;
    }

    this.cache.set(key, node);

    // Evict if over capacity
    if (this.cache.size > this.maxSize) {
      this.evict();
    }
  }

  /**
   * Delete a value from the cache
   * O(1) time complexity
   *
   * @param key - Cache key
   * @returns true if key existed and was deleted, false otherwise
   */
  delete(key: K): boolean {
    const node = this.cache.get(key);
    if (!node) {
      return false;
    }

    this.removeNode(node);
    this.cache.delete(key);
    return true;
  }

  /**
   * Check if a key exists in the cache
   * O(1) time complexity
   *
   * @param key - Cache key
   * @returns true if key exists, false otherwise
   */
  has(key: K): boolean {
    return this.cache.has(key);
  }

  /**
   * Clear all entries from the cache
   */
  clear(): void {
    this.cache.clear();
    this.head = null;
    this.tail = null;
  }

  /**
   * Get the current number of cached items
   */
  get size(): number {
    return this.cache.size;
  }

  /**
   * Get the maximum cache size
   */
  get max(): number {
    return this.maxSize;
  }

  /**
   * Move a node to the front of the list (mark as most recently used)
   */
  private moveToFront(node: LRUNode<K, V>): void {
    if (node === this.head) {
      return; // Already at front
    }

    // Remove from current position
    this.removeNode(node);

    // Add to front
    if (!this.head) {
      this.head = node;
      this.tail = node;
    } else {
      node.next = this.head;
      this.head.prev = node;
      this.head = node;
    }
  }

  /**
   * Remove a node from the linked list
   */
  private removeNode(node: LRUNode<K, V>): void {
    if (node.prev) {
      node.prev.next = node.next;
    } else {
      // Node is head
      this.head = node.next;
    }

    if (node.next) {
      node.next.prev = node.prev;
    } else {
      // Node is tail
      this.tail = node.prev;
    }

    node.prev = null;
    node.next = null;
  }

  /**
   * Evict the least recently used item (tail of the list)
   */
  private evict(): void {
    if (!this.tail) {
      return;
    }

    const key = this.tail.key;
    this.removeNode(this.tail);
    this.cache.delete(key);
  }
}

