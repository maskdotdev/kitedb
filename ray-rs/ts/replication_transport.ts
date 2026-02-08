import {
  collectReplicationLogTransportJson,
  collectReplicationMetricsOtelJson,
  collectReplicationMetricsPrometheus,
  collectReplicationSnapshotTransportJson,
} from '../index'
import type { Database } from '../index'

export interface ReplicationSnapshotTransport {
  format: string
  db_path: string
  byte_length: number
  checksum_crc32c: string
  generated_at_ms: number
  epoch: number
  head_log_index: number
  retained_floor: number
  start_cursor: string
  data_base64?: string | null
}

export interface ReplicationLogTransportFrame {
  epoch: number
  log_index: number
  segment_id: number
  segment_offset: number
  bytes: number
  payload_base64?: string | null
}

export interface ReplicationLogTransportPage {
  epoch: number
  head_log_index: number
  retained_floor: number
  cursor?: string | null
  next_cursor?: string | null
  eof: boolean
  frame_count: number
  total_bytes: number
  frames: ReplicationLogTransportFrame[]
}

export interface ReplicationLogTransportOptions {
  cursor?: string | null
  maxFrames?: number
  maxBytes?: number
  includePayload?: boolean
}

export interface ReplicationTransportAdapter {
  snapshot(includeData?: boolean): ReplicationSnapshotTransport
  log(options?: ReplicationLogTransportOptions): ReplicationLogTransportPage
  metricsPrometheus(): string
  metricsOtelJson(): string
}

function parseJson<T>(raw: string, label: string): T {
  try {
    return JSON.parse(raw) as T
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`Failed to parse ${label}: ${message}`)
  }
}

export function readReplicationSnapshotTransport(
  db: Database,
  includeData = false,
): ReplicationSnapshotTransport {
  const raw = collectReplicationSnapshotTransportJson(db, includeData)
  return parseJson<ReplicationSnapshotTransport>(raw, 'replication snapshot transport JSON')
}

export function readReplicationLogTransport(
  db: Database,
  options: ReplicationLogTransportOptions = {},
): ReplicationLogTransportPage {
  const raw = collectReplicationLogTransportJson(
    db,
    options.cursor ?? null,
    options.maxFrames ?? 128,
    options.maxBytes ?? 1024 * 1024,
    options.includePayload ?? true,
  )
  return parseJson<ReplicationLogTransportPage>(raw, 'replication log transport JSON')
}

export function createReplicationTransportAdapter(db: Database): ReplicationTransportAdapter {
  return {
    snapshot(includeData = false): ReplicationSnapshotTransport {
      return readReplicationSnapshotTransport(db, includeData)
    },
    log(options: ReplicationLogTransportOptions = {}): ReplicationLogTransportPage {
      return readReplicationLogTransport(db, options)
    },
    metricsPrometheus(): string {
      return collectReplicationMetricsPrometheus(db)
    },
    metricsOtelJson(): string {
      return collectReplicationMetricsOtelJson(db)
    },
  }
}
