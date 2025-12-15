/**
 * MinHeap Priority Queue
 *
 * Efficient binary heap implementation for pathfinding algorithms.
 * O(log n) insert and extract-min operations.
 */

interface HeapItem<T> {
  item: T;
  priority: number;
}

/**
 * Min-heap priority queue
 *
 * Maintains items in a min-heap structure where the item with the lowest
 * priority value is always at the root.
 */
export class MinHeap<T> {
  private heap: HeapItem<T>[] = [];
  private itemToIndex: Map<T, number> = new Map();

  /**
   * Insert an item with a priority
   * O(log n) time complexity
   */
  insert(item: T, priority: number): void {
    const heapItem: HeapItem<T> = { item, priority };
    const index = this.heap.length;
    this.heap.push(heapItem);
    this.itemToIndex.set(item, index);
    this.bubbleUp(index);
  }

  /**
   * Extract and return the item with minimum priority
   * Returns undefined if heap is empty
   * O(log n) time complexity
   */
  extractMin(): T | undefined {
    if (this.heap.length === 0) {
      return undefined;
    }

    const min = this.heap[0]!.item;
    const last = this.heap.pop()!;

    if (this.heap.length > 0) {
      this.heap[0] = last;
      this.itemToIndex.set(last.item, 0);
      this.bubbleDown(0);
    }

    this.itemToIndex.delete(min);
    return min;
  }

  /**
   * Decrease the priority of an item
   * If the item is not in the heap, inserts it with the new priority
   * O(log n) time complexity
   */
  decreasePriority(item: T, newPriority: number): void {
    const index = this.itemToIndex.get(item);
    if (index === undefined) {
      // Item not in heap, insert it
      this.insert(item, newPriority);
      return;
    }

    const currentPriority = this.heap[index]!.priority;
    if (newPriority >= currentPriority) {
      // Not a decrease, ignore
      return;
    }

    this.heap[index]!.priority = newPriority;
    this.bubbleUp(index);
  }

  /**
   * Check if the heap is empty
   */
  isEmpty(): boolean {
    return this.heap.length === 0;
  }

  /**
   * Get the current size of the heap
   */
  size(): number {
    return this.heap.length;
  }

  /**
   * Check if an item exists in the heap
   */
  has(item: T): boolean {
    return this.itemToIndex.has(item);
  }

  /**
   * Get the priority of an item, or undefined if not in heap
   */
  getPriority(item: T): number | undefined {
    const index = this.itemToIndex.get(item);
    if (index === undefined) {
      return undefined;
    }
    return this.heap[index]!.priority;
  }

  /**
   * Move an item up the heap to maintain min-heap property
   */
  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2);
      if (this.heap[parentIndex]!.priority <= this.heap[index]!.priority) {
        break;
      }

      this.swap(index, parentIndex);
      index = parentIndex;
    }
  }

  /**
   * Move an item down the heap to maintain min-heap property
   */
  private bubbleDown(index: number): void {
    while (true) {
      let smallest = index;
      const left = 2 * index + 1;
      const right = 2 * index + 2;

      if (
        left < this.heap.length &&
        this.heap[left]!.priority < this.heap[smallest]!.priority
      ) {
        smallest = left;
      }

      if (
        right < this.heap.length &&
        this.heap[right]!.priority < this.heap[smallest]!.priority
      ) {
        smallest = right;
      }

      if (smallest === index) {
        break;
      }

      this.swap(index, smallest);
      index = smallest;
    }
  }

  /**
   * Swap two items in the heap
   */
  private swap(i: number, j: number): void {
    const temp = this.heap[i]!;
    this.heap[i] = this.heap[j]!;
    this.heap[j] = temp;

    this.itemToIndex.set(this.heap[i]!.item, i);
    this.itemToIndex.set(this.heap[j]!.item, j);
  }
}
