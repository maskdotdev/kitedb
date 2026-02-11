/**
 * Host-runtime replication HTTP adapter template (generic middleware).
 *
 * Purpose:
 * - framework-agnostic route handler factory
 * - plug into Express/Fastify/Hono/Elysia adapters
 * - reuse transport JSON helpers from `ray-rs/ts/replication_transport.ts`
 */

import {
  createReplicationTransportAdapter,
  type ReplicationLogTransportOptions,
  type ReplicationTransportAdapter,
} from '../../ray-rs/ts/replication_transport'
import type { Database } from '../../ray-rs/index'

type RequestLike = {
  method: string
  path: string
  query: Record<string, string | undefined>
  headers: Record<string, string | undefined>
  body?: unknown
}

type ResponseLike = {
  status: number
  headers?: Record<string, string>
  body: unknown
}

type RequireAdmin = (request: RequestLike) => void

function parseBool(raw: string | undefined, fallback: boolean): boolean {
  if (raw === undefined) return fallback
  const normalized = raw.trim().toLowerCase()
  if (normalized === '1' || normalized === 'true' || normalized === 'yes') return true
  if (normalized === '0' || normalized === 'false' || normalized === 'no') return false
  return fallback
}

function parsePositiveInt(raw: string | undefined, fallback: number, max: number): number {
  if (raw === undefined || raw.trim() === '') return fallback
  const parsed = Number(raw)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.floor(parsed), 1), max)
}

export function createReplicationMiddleware(
  db: Database,
  requireAdmin: RequireAdmin,
): (request: RequestLike) => ResponseLike {
  const adapter: ReplicationTransportAdapter = createReplicationTransportAdapter(db)

  return (request: RequestLike): ResponseLike => {
    const path = request.path
    try {
      if (path === '/replication/status') {
        return {
          status: 200,
          body: {
            primary: db.primaryReplicationStatus(),
            replica: db.replicaReplicationStatus(),
          },
        }
      }

      if (path === '/replication/metrics/prometheus') {
        requireAdmin(request)
        return {
          status: 200,
          headers: { 'content-type': 'text/plain; charset=utf-8' },
          body: adapter.metricsPrometheus(),
        }
      }

      if (path === '/replication/metrics/otel-json') {
        requireAdmin(request)
        return { status: 200, body: JSON.parse(adapter.metricsOtelJson()) }
      }

      if (path === '/replication/transport/snapshot') {
        requireAdmin(request)
        const includeData = parseBool(request.query.includeData, false)
        return { status: 200, body: adapter.snapshot(includeData) }
      }

      if (path === '/replication/transport/log') {
        requireAdmin(request)
        const options: ReplicationLogTransportOptions = {
          cursor: request.query.cursor ?? null,
          maxFrames: parsePositiveInt(request.query.maxFrames, 128, 10_000),
          maxBytes: parsePositiveInt(request.query.maxBytes, 1024 * 1024, 32 * 1024 * 1024),
          includePayload: parseBool(request.query.includePayload, true),
        }
        return { status: 200, body: adapter.log(options) }
      }

      if (path === '/replication/pull' && request.method === 'POST') {
        requireAdmin(request)
        const maxFrames = Number(
          (request.body as { maxFrames?: number } | undefined)?.maxFrames ?? 256,
        )
        const appliedFrames = db.replicaCatchUpOnce(Math.max(1, maxFrames))
        return {
          status: 200,
          body: { appliedFrames, replica: db.replicaReplicationStatus() },
        }
      }

      if (path === '/replication/reseed' && request.method === 'POST') {
        requireAdmin(request)
        db.replicaReseedFromSnapshot()
        return { status: 200, body: { replica: db.replicaReplicationStatus() } }
      }

      if (path === '/replication/promote' && request.method === 'POST') {
        requireAdmin(request)
        const epoch = db.primaryPromoteToNextEpoch()
        return {
          status: 200,
          body: { epoch, primary: db.primaryReplicationStatus() },
        }
      }

      return { status: 404, body: { error: 'not found' } }
    } catch (error) {
      return {
        status: 500,
        body: { error: error instanceof Error ? error.message : String(error) },
      }
    }
  }
}

/**
 * Example auth callback:
 * const token = process.env.REPLICATION_ADMIN_TOKEN ?? ''
 * const requireAdmin: RequireAdmin = (request) => {
 *   if (!token) return
 *   if (request.headers.authorization !== `Bearer ${token}`) {
 *     throw new Error('unauthorized')
 *   }
 * }
 */
