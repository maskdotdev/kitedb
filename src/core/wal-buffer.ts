/**
 * Circular WAL buffer for single-file format with dual-region support
 * 
 * The WAL uses a circular buffer design within the database file.
 * Records wrap around when reaching the end of the WAL area.
 * Checkpoint advances the tail pointer to reclaim space.
 * 
 * Dual-Region Mode (for background checkpointing):
 * - Primary region: 75% of WAL space (normal writes)
 * - Secondary region: 25% of WAL space (writes during checkpoint)
 * 
 * Optimization: Uses page-level write batching to reduce I/O amplification.
 * Instead of writing each small record individually (causing read-modify-write
 * for each ~100 byte record on a 4KB page), we buffer writes in memory and
 * flush entire pages at once.
 */

import { WAL_RECORD_ALIGNMENT } from "../constants.ts";
import { WalBufferFullError, type DbHeaderV1 } from "../types.ts";
import { alignUp, paddingFor, viewOf, writeU32 } from "../util/binary.ts";
import { crc32c } from "../util/crc.ts";
import type { FilePager } from "./pager.ts";
import { buildWalRecord, parseWalRecord, type ParsedWalRecord, type WalRecord } from "./wal.ts";

/** WAL region split ratio: primary gets 75%, secondary gets 25% */
const PRIMARY_REGION_RATIO = 0.75;
const SECONDARY_REGION_RATIO = 0.25;

/**
 * Circular WAL buffer manager with page-level write batching and dual-region support
 */
export class WalBuffer {
  private pager: FilePager;
  private walStartOffset: number; // Byte offset of WAL area start in file
  private walSize: number; // Total WAL area size in bytes
  private head: number; // Current write position (bytes from WAL start)
  private tail: number; // Oldest uncompacted position (bytes from WAL start)
  
  // Dual-region support for background checkpointing
  private primaryRegionSize: number;   // Size of primary region (75%)
  private secondaryRegionStart: number; // Start offset of secondary region
  private secondaryRegionSize: number;  // Size of secondary region (25%)
  private activeRegion: 0 | 1;          // 0=primary, 1=secondary
  private primaryHead: number;          // Primary region write position
  private secondaryHead: number;        // Secondary region write position
  
  // Page-level write batching - reduces I/O amplification
  // Map from page number -> modified page buffer
  private pendingWrites: Map<number, Uint8Array> = new Map();
  
  constructor(
    pager: FilePager,
    header: DbHeaderV1,
  ) {
    this.pager = pager;
    this.walStartOffset = Number(header.walStartPage) * header.pageSize;
    this.walSize = Number(header.walPageCount) * header.pageSize;
    this.head = Number(header.walHead);
    this.tail = Number(header.walTail);
    
    // Calculate region sizes
    this.primaryRegionSize = Math.floor(this.walSize * PRIMARY_REGION_RATIO);
    this.secondaryRegionStart = this.primaryRegionSize;
    this.secondaryRegionSize = this.walSize - this.primaryRegionSize;
    
    // Initialize from header V2 fields
    this.activeRegion = header.activeWalRegion;
    this.primaryHead = Number(header.walPrimaryHead);
    this.secondaryHead = Number(header.walSecondaryHead);
    
    // Initialize secondaryHead to its start position if not set
    if (this.secondaryHead === 0) {
      this.secondaryHead = this.secondaryRegionStart;
    }
    
    // For backward compatibility: if V2 fields are 0 and head is non-zero,
    // initialize primaryHead from head
    if (this.primaryHead === 0 && this.head > 0) {
      this.primaryHead = this.head;
    }
  }

  /**
   * Get current head offset (bytes from WAL start)
   */
  getHead(): bigint {
    return BigInt(this.head);
  }

  /**
   * Get current tail offset (bytes from WAL start)
   */
  getTail(): bigint {
    return BigInt(this.tail);
  }
  
  /**
   * Get primary region head (bytes from WAL start)
   */
  getPrimaryHead(): bigint {
    return BigInt(this.primaryHead);
  }
  
  /**
   * Get secondary region head (bytes from WAL start)
   */
  getSecondaryHead(): bigint {
    return BigInt(this.secondaryHead);
  }
  
  /**
   * Get active region (0=primary, 1=secondary)
   */
  getActiveRegion(): 0 | 1 {
    return this.activeRegion;
  }
  
  /**
   * Get primary region size in bytes
   */
  getPrimaryRegionSize(): number {
    return this.primaryRegionSize;
  }
  
  /**
   * Get secondary region size in bytes
   */
  getSecondaryRegionSize(): number {
    return this.secondaryRegionSize;
  }

  /**
   * Calculate used space in the circular buffer (for primary region only when active)
   */
  getUsedSpace(): number {
    if (this.activeRegion === 0) {
      // Primary region: simple linear usage
      return this.primaryHead - this.tail;
    }
    // Secondary region: usage is just secondary head
    return this.secondaryHead - this.secondaryRegionStart;
  }

  /**
   * Calculate available space in the active region
   */
  getAvailableSpace(): number {
    if (this.activeRegion === 0) {
      // Primary region available space
      return this.primaryRegionSize - this.primaryHead - 1;
    }
    // Secondary region available space
    return this.secondaryRegionSize - (this.secondaryHead - this.secondaryRegionStart) - 1;
  }
  
  /**
   * Get usage ratio for the active region (0.0 to 1.0)
   */
  getActiveRegionUsage(): number {
    if (this.activeRegion === 0) {
      return this.primaryHead / this.primaryRegionSize;
    }
    return (this.secondaryHead - this.secondaryRegionStart) / this.secondaryRegionSize;
  }
  
  /**
   * Get usage ratio for secondary region (0.0 to 1.0)
   */
  getSecondaryRegionUsage(): number {
    return (this.secondaryHead - this.secondaryRegionStart) / this.secondaryRegionSize;
  }

  /**
   * Check if we can write a record of given size
   */
  canWrite(recordSize: number): boolean {
    // Add alignment
    const alignedSize = alignUp(recordSize, WAL_RECORD_ALIGNMENT);
    return alignedSize <= this.getAvailableSpace();
  }

  /**
   * Check if writing would cause wrap-around (only relevant for primary region)
   */
  wouldWrapAround(recordSize: number): boolean {
    if (this.activeRegion === 1) {
      // Secondary region doesn't wrap
      return false;
    }
    const alignedSize = alignUp(recordSize, WAL_RECORD_ALIGNMENT);
    return this.primaryHead + alignedSize > this.primaryRegionSize;
  }
  
  /**
   * Switch writes to secondary region (called when starting background checkpoint)
   */
  switchToSecondary(): void {
    if (this.activeRegion === 1) {
      return; // Already in secondary
    }
    this.activeRegion = 1;
    // Update head to track active position
    this.head = this.secondaryHead;
  }
  
  /**
   * Switch writes back to primary region (called after checkpoint completes)
   * Optionally merges secondary records into the new primary
   */
  switchToPrimary(resetPrimary: boolean = true): void {
    if (this.activeRegion === 0 && !resetPrimary) {
      return; // Already in primary and no reset needed
    }
    this.activeRegion = 0;
    if (resetPrimary) {
      // Reset primary head (checkpoint completed, WAL is cleared)
      this.primaryHead = 0;
      this.tail = 0;
    }
    // Update head to track active position
    this.head = this.primaryHead;
  }
  
  /**
   * Merge secondary region records into primary region
   * Called after checkpoint completes to preserve any writes that occurred during checkpoint
   */
  mergeSecondaryIntoPrimary(): void {
    // Read all records from secondary region (if any)
    const hasSecondaryRecords = this.secondaryHead > this.secondaryRegionStart;
    const secondaryRecords = hasSecondaryRecords ? this.scanRegion(1) : [];
    
    // Reset both regions - this must happen even if no secondary records exist,
    // because checkpoint has incorporated all primary WAL data into the snapshot
    this.primaryHead = 0;
    this.secondaryHead = this.secondaryRegionStart;
    this.tail = 0;
    this.activeRegion = 0;
    this.head = 0;
    
    // Re-write secondary records to primary region
    for (const record of secondaryRecords) {
      // Rebuild the record and write it
      const recordBytes = buildWalRecord({
        type: record.type,
        txid: record.txid,
        payload: record.payload,
      });
      this.writeRecordBytes(recordBytes);
    }
  }
  
  /**
   * Scan records from a specific region
   * @param region 0 for primary, 1 for secondary
   */
  scanRegion(region: 0 | 1): ParsedWalRecord[] {
    const records: ParsedWalRecord[] = [];
    
    let pos: number;
    let endPos: number;
    let regionStart: number;
    
    if (region === 0) {
      pos = this.tail;
      endPos = this.primaryHead;
      regionStart = 0;
    } else {
      pos = this.secondaryRegionStart;
      endPos = this.secondaryHead;
      regionStart = this.secondaryRegionStart;
    }
    
    while (pos < endPos) {
      const fileOffset = this.walStartOffset + pos;
      const headerBytes = this.readAtOffset(fileOffset, 8);
      const headerView = viewOf(headerBytes);
      
      const recLen = headerView.getUint32(0, true);
      
      // Check for skip marker
      if (recLen === 0) {
        const marker = headerView.getUint32(4, true);
        if (marker === 0xFFFFFFFF) {
          // Skip to start of region
          pos = regionStart;
          continue;
        }
        // Invalid record
        break;
      }
      
      // Calculate total record size with alignment
      const padLen = paddingFor(recLen, WAL_RECORD_ALIGNMENT);
      const totalLen = recLen + padLen;
      
      const recordBuffer = this.readAtOffset(fileOffset, totalLen);
      
      // Parse the record
      const record = parseWalRecord(recordBuffer, 0);
      if (!record) {
        break;
      }
      
      records.push(record);
      pos += totalLen;
    }
    
    return records;
  }

  /**
   * Write a WAL record to the circular buffer (batched)
   * Returns the new head position
   * 
   * Note: Records are buffered in memory. Call flush() to write to disk.
   * 
   * @throws WalBufferFullError if buffer is full
   */
  writeRecord(record: WalRecord): number {
    const recordBytes = buildWalRecord(record);
    return this.writeRecordBytes(recordBytes);
  }
  
  /**
   * Write raw record bytes to the active region
   */
  private writeRecordBytes(recordBytes: Uint8Array): number {
    const recordSize = recordBytes.length;
    
    if (!this.canWrite(recordSize)) {
      throw new WalBufferFullError();
    }

    if (this.activeRegion === 0) {
      // Primary region
      if (this.wouldWrapAround(recordSize)) {
        // Write a skip marker at current position and wrap to start
        this.writeSkipMarker();
        this.primaryHead = 0;
      }

      // Calculate file offset
      const fileOffset = this.walStartOffset + this.primaryHead;
      
      // Buffer the write for later flushing
      this.bufferWrite(fileOffset, recordBytes);
      
      // Update head
      this.primaryHead = this.primaryHead + recordSize;
      this.head = this.primaryHead;
    } else {
      // Secondary region
      const fileOffset = this.walStartOffset + this.secondaryHead;
      
      // Buffer the write
      this.bufferWrite(fileOffset, recordBytes);
      
      // Update head
      this.secondaryHead = this.secondaryHead + recordSize;
      this.head = this.secondaryHead;
    }
    
    return this.head;
  }

  /**
   * Write a skip marker to indicate end of valid data before wrap
   */
  private writeSkipMarker(): void {
    // A skip marker is a special record with zero length
    // It tells the reader to skip to the start of the buffer
    const marker = new Uint8Array(8);
    const view = viewOf(marker);
    writeU32(view, 0, 0); // recLen = 0 means skip
    writeU32(view, 4, 0xFFFFFFFF); // Magic skip marker
    
    const fileOffset = this.walStartOffset + this.head;
    this.bufferWrite(fileOffset, marker);
  }
  
  /**
   * Buffer a write for later flushing (page-level batching)
   * This reduces I/O amplification by accumulating writes to the same page
   */
  private bufferWrite(offset: number, data: Uint8Array): void {
    const pageSize = this.pager.pageSize;
    const startPage = Math.floor(offset / pageSize);
    const endPage = Math.floor((offset + data.length - 1) / pageSize);
    
    let dataOffset = 0;
    for (let pageNum = startPage; pageNum <= endPage; pageNum++) {
      // Get or create the page buffer
      let pageBuffer = this.pendingWrites.get(pageNum);
      if (!pageBuffer) {
        // First write to this page - load existing content
        pageBuffer = this.pager.readPage(pageNum);
        // Create a copy so we can modify it
        pageBuffer = new Uint8Array(pageBuffer);
        this.pendingWrites.set(pageNum, pageBuffer);
      }
      
      const pageStart = pageNum * pageSize;
      const pageEnd = pageStart + pageSize;
      
      const writeStart = Math.max(offset, pageStart);
      const writeEnd = Math.min(offset + data.length, pageEnd);
      const writeLen = writeEnd - writeStart;
      
      const pageWriteOffset = writeStart - pageStart;
      pageBuffer.set(data.subarray(dataOffset, dataOffset + writeLen), pageWriteOffset);
      
      dataOffset += writeLen;
    }
  }

  /**
   * Write bytes at a specific file offset (immediate write, bypasses batching)
   * Used for reads that need immediate consistency
   */
  private writeAtOffset(offset: number, data: Uint8Array): void {
    // Calculate which pages are affected
    const pageSize = this.pager.pageSize;
    const startPage = Math.floor(offset / pageSize);
    const endPage = Math.floor((offset + data.length - 1) / pageSize);
    
    // For small writes within a single page
    if (startPage === endPage) {
      const page = this.pager.readPage(startPage);
      const pageOffset = offset % pageSize;
      page.set(data, pageOffset);
      this.pager.writePage(startPage, page);
      return;
    }
    
    // For writes spanning multiple pages
    let dataOffset = 0;
    for (let pageNum = startPage; pageNum <= endPage; pageNum++) {
      const page = this.pager.readPage(pageNum);
      const pageStart = pageNum * pageSize;
      const pageEnd = pageStart + pageSize;
      
      const writeStart = Math.max(offset, pageStart);
      const writeEnd = Math.min(offset + data.length, pageEnd);
      const writeLen = writeEnd - writeStart;
      
      const pageWriteOffset = writeStart - pageStart;
      page.set(data.subarray(dataOffset, dataOffset + writeLen), pageWriteOffset);
      this.pager.writePage(pageNum, page);
      
      dataOffset += writeLen;
    }
  }

  /**
   * Read bytes from a specific file offset
   * If there are pending writes, reads from the buffered data
   */
  private readAtOffset(offset: number, length: number): Uint8Array {
    const pageSize = this.pager.pageSize;
    const startPage = Math.floor(offset / pageSize);
    const endPage = Math.floor((offset + length - 1) / pageSize);
    
    // For reads within a single page
    if (startPage === endPage) {
      // Check for pending writes first
      const pendingPage = this.pendingWrites.get(startPage);
      const page = pendingPage ?? this.pager.readPage(startPage);
      const pageOffset = offset % pageSize;
      return page.subarray(pageOffset, pageOffset + length);
    }
    
    // For reads spanning multiple pages
    const result = new Uint8Array(length);
    let resultOffset = 0;
    
    for (let pageNum = startPage; pageNum <= endPage; pageNum++) {
      // Check for pending writes first
      const pendingPage = this.pendingWrites.get(pageNum);
      const page = pendingPage ?? this.pager.readPage(pageNum);
      const pageStart = pageNum * pageSize;
      const pageEnd = pageStart + pageSize;
      
      const readStart = Math.max(offset, pageStart);
      const readEnd = Math.min(offset + length, pageEnd);
      const readLen = readEnd - readStart;
      
      const pageReadOffset = readStart - pageStart;
      result.set(page.subarray(pageReadOffset, pageReadOffset + readLen), resultOffset);
      
      resultOffset += readLen;
    }
    
    return result;
  }
  
  /**
   * Flush all pending writes to disk
   * This writes all buffered pages in a single batch
   */
  flushPendingWrites(): void {
    for (const [pageNum, data] of this.pendingWrites) {
      this.pager.writePage(pageNum, data);
    }
    this.pendingWrites.clear();
  }
  
  /**
   * Check if there are pending writes
   */
  hasPendingWrites(): boolean {
    return this.pendingWrites.size > 0;
  }

  /**
   * Advance tail after checkpoint
   */
  advanceTail(newTail: number): void {
    this.tail = newTail % this.walSize;
  }

  /**
   * Scan all valid records from tail to head
   */
  scanRecords(): ParsedWalRecord[] {
    const records: ParsedWalRecord[] = [];
    
    let pos = this.tail;
    while (pos !== this.head) {
      // Read enough bytes for header
      const fileOffset = this.walStartOffset + pos;
      const headerBytes = this.readAtOffset(fileOffset, 8);
      const headerView = viewOf(headerBytes);
      
      const recLen = headerView.getUint32(0, true);
      
      // Check for skip marker
      if (recLen === 0) {
        const marker = headerView.getUint32(4, true);
        if (marker === 0xFFFFFFFF) {
          // Skip to start
          pos = 0;
          continue;
        }
        // Invalid record
        break;
      }
      
      // Calculate total record size with alignment
      const padLen = paddingFor(recLen, WAL_RECORD_ALIGNMENT);
      const totalLen = recLen + padLen;
      
      // Check for wrap-around read
      let recordBuffer: Uint8Array;
      if (pos + totalLen > this.walSize) {
        // This shouldn't happen with proper skip markers, but handle it
        break;
      }
      
      recordBuffer = this.readAtOffset(fileOffset, totalLen);
      
      // Parse the record
      const record = parseWalRecord(recordBuffer, 0);
      if (!record) {
        // Invalid record, stop scanning
        break;
      }
      
      records.push(record);
      pos = (pos + totalLen) % this.walSize;
    }
    
    return records;
  }

  /**
   * Get records for recovery (from both regions if checkpoint was in progress)
   */
  getRecordsForRecovery(): ParsedWalRecord[] {
    // Scan primary region first
    const primaryRecords = this.scanRegion(0);
    
    // If checkpoint was in progress (secondary region has data), include those too
    if (this.secondaryHead > this.secondaryRegionStart) {
      const secondaryRecords = this.scanRegion(1);
      return [...primaryRecords, ...secondaryRecords];
    }
    
    return primaryRecords;
  }

  /**
   * Clear the WAL buffer (after successful compaction)
   */
  clear(): void {
    this.head = 0;
    this.tail = 0;
    this.primaryHead = 0;
    this.secondaryHead = this.secondaryRegionStart;
    this.activeRegion = 0;
    this.pendingWrites.clear();
  }

  /**
   * Sync WAL to disk (flushes pending writes first)
   */
  async sync(): Promise<void> {
    this.flushPendingWrites();
    await this.pager.sync();
  }

  /**
   * Sync WAL to disk (synchronous, flushes pending writes first)
   */
  syncSync(): void {
    this.flushPendingWrites();
    this.pager.syncSync();
  }
}

/**
 * Create a new WAL buffer from header
 */
export function createWalBuffer(pager: FilePager, header: DbHeaderV1): WalBuffer {
  return new WalBuffer(pager, header);
}

/**
 * Calculate minimum WAL size based on snapshot size
 * Uses the larger of 1MB or 10% of snapshot size
 */
export function calculateWalSize(snapshotSize: number): number {
  const minSize = 1 * 1024 * 1024; // 1MB
  const ratioSize = Math.ceil(snapshotSize * 0.1);
  return Math.max(minSize, ratioSize);
}

/**
 * Convert WAL size to page count
 */
export function walSizeToPageCount(walSize: number, pageSize: number): number {
  return Math.ceil(walSize / pageSize);
}
