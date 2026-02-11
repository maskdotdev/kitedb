/**
 * Host-runtime replication HTTP adapter (Node + Express).
 *
 * Purpose:
 * - production-style non-playground embedding
 * - end-to-end status/admin/transport wiring
 * - token + optional Node TLS mTLS auth via helper APIs
 *
 * Run:
 *   npm i express
 *   export REPLICATION_ADMIN_AUTH_MODE=token_or_mtls
 *   export REPLICATION_ADMIN_TOKEN=change-me
 *   tsx replication_adapter_node_express.ts
 */

import express, { type Request, type Response } from 'express'

import { Database } from '../../ray-rs/index'
import {
  createNodeTlsMtlsMatcher,
  createReplicationAdminAuthorizer,
  createReplicationTransportAdapter,
  type ReplicationAdminAuthMode,
  type ReplicationAdminAuthRequest,
  type ReplicationTransportAdapter,
} from '../../ray-rs/ts/replication_transport'

type RequestLike = ReplicationAdminAuthRequest & {
  socket?: { authorized?: boolean }
  client?: { authorized?: boolean }
  raw?: { socket?: { authorized?: boolean } }
  req?: { socket?: { authorized?: boolean } }
}

function parseBool(raw: unknown, fallback: boolean): boolean {
  if (raw === undefined || raw === null) return fallback
  const normalized = String(raw).trim().toLowerCase()
  if (['1', 'true', 'yes'].includes(normalized)) return true
  if (['0', 'false', 'no'].includes(normalized)) return false
  return fallback
}

function parsePositiveInt(raw: unknown, fallback: number, max: number): number {
  if (raw === undefined || raw === null) return fallback
  const parsed = Number(raw)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.floor(parsed), 1), max)
}

const DB_PATH = process.env.KITEDB_PATH ?? 'cluster-primary.kitedb'
const SIDECAR_PATH = process.env.KITEDB_REPLICATION_SIDECAR ?? 'cluster-primary.sidecar'
const PORT = parsePositiveInt(process.env.PORT, 8080, 65535)
const AUTH_MODE =
  (process.env.REPLICATION_ADMIN_AUTH_MODE as ReplicationAdminAuthMode | undefined) ??
  'token_or_mtls'
const AUTH_TOKEN = process.env.REPLICATION_ADMIN_TOKEN ?? ''

const db = Database.open(DB_PATH, {
  replicationRole: 'Primary',
  replicationSidecarPath: SIDECAR_PATH,
})

const adapter: ReplicationTransportAdapter = createReplicationTransportAdapter(db)
const requireAdmin = createReplicationAdminAuthorizer<RequestLike>({
  mode: AUTH_MODE,
  token: AUTH_TOKEN,
  mtlsMatcher: createNodeTlsMtlsMatcher({ requirePeerCertificate: false }),
})

const app = express()
app.use(express.json({ limit: '2mb' }))

function checked(handler: (req: Request, res: Response) => void) {
  return (req: Request, res: Response) => {
    try {
      handler(req, res)
    } catch (error) {
      res.status(500).json({ error: error instanceof Error ? error.message : String(error) })
    }
  }
}

function ensureAdmin(req: Request): void {
  requireAdmin({
    headers: req.headers as Record<string, string | undefined>,
    socket: req.socket as RequestLike['socket'],
    client: (req as unknown as { client?: RequestLike['client'] }).client,
    raw: (req as unknown as { raw?: RequestLike['raw'] }).raw,
    req: (req as unknown as { req?: RequestLike['req'] }).req,
  })
}

app.get(
  '/replication/status',
  checked((_req, res) => {
    res.json({
      primary: db.primaryReplicationStatus(),
      replica: db.replicaReplicationStatus(),
    })
  }),
)

app.get(
  '/replication/metrics/prometheus',
  checked((req, res) => {
    ensureAdmin(req)
    res.type('text/plain').send(adapter.metricsPrometheus())
  }),
)

app.get(
  '/replication/metrics/otel-json',
  checked((req, res) => {
    ensureAdmin(req)
    res.json(JSON.parse(adapter.metricsOtelJson()))
  }),
)

app.get(
  '/replication/transport/snapshot',
  checked((req, res) => {
    ensureAdmin(req)
    const includeData = parseBool(req.query.includeData, false)
    res.json(adapter.snapshot(includeData))
  }),
)

app.get(
  '/replication/transport/log',
  checked((req, res) => {
    ensureAdmin(req)
    res.json(
      adapter.log({
        cursor: (req.query.cursor as string | undefined) ?? null,
        maxFrames: parsePositiveInt(req.query.maxFrames, 128, 10_000),
        maxBytes: parsePositiveInt(req.query.maxBytes, 1024 * 1024, 32 * 1024 * 1024),
        includePayload: parseBool(req.query.includePayload, true),
      }),
    )
  }),
)

app.post(
  '/replication/pull',
  checked((req, res) => {
    ensureAdmin(req)
    const maxFrames = parsePositiveInt(req.body?.maxFrames, 256, 100_000)
    const appliedFrames = db.replicaCatchUpOnce(maxFrames)
    res.json({ appliedFrames, replica: db.replicaReplicationStatus() })
  }),
)

app.post(
  '/replication/reseed',
  checked((req, res) => {
    ensureAdmin(req)
    db.replicaReseedFromSnapshot()
    res.json({ replica: db.replicaReplicationStatus() })
  }),
)

app.post(
  '/replication/promote',
  checked((req, res) => {
    ensureAdmin(req)
    const epoch = db.primaryPromoteToNextEpoch()
    res.json({ epoch, primary: db.primaryReplicationStatus() })
  }),
)

const server = app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`replication adapter listening on http://127.0.0.1:${PORT}`)
})

function shutdown() {
  server.close(() => {
    try {
      db.close()
    } catch {}
    process.exit(0)
  })
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)
