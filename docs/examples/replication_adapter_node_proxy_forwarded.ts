/**
 * Host-runtime replication HTTP adapter (Node + Express behind reverse proxy).
 *
 * Purpose:
 * - production-style embedding when TLS/mTLS terminates at ingress/proxy
 * - forwarded-header mTLS verification + optional token auth
 * - end-to-end status/admin/transport wiring
 *
 * Run:
 *   npm i express
 *   export REPLICATION_ADMIN_AUTH_MODE=token_or_mtls
 *   export REPLICATION_ADMIN_TOKEN=change-me
 *   export REPLICATION_MTLS_SUBJECT_REGEX='^CN=replication-admin,'
 *   tsx replication_adapter_node_proxy_forwarded.ts
 */

import express, { type Request, type Response } from 'express'

import { Database } from '../../ray-rs/index'
import {
  createForwardedTlsMtlsMatcher,
  createReplicationAdminAuthorizer,
  createReplicationTransportAdapter,
  type ReplicationAdminAuthMode,
  type ReplicationAdminAuthRequest,
  type ReplicationForwardedMtlsMatcherOptions,
  type ReplicationTransportAdapter,
} from '../../ray-rs/ts/replication_transport'

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

function readHeader(headers: Record<string, string | undefined>, name: string): string | null {
  const direct = headers[name]
  if (direct && direct.trim().length > 0) return direct.trim()
  const target = name.toLowerCase()
  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() !== target) continue
    if (typeof value !== 'string') continue
    const trimmed = value.trim()
    if (trimmed.length > 0) return trimmed
  }
  return null
}

const DB_PATH = process.env.KITEDB_PATH ?? 'cluster-primary.kitedb'
const SIDECAR_PATH = process.env.KITEDB_REPLICATION_SIDECAR ?? 'cluster-primary.sidecar'
const PORT = parsePositiveInt(process.env.PORT, 8081, 65535)
const AUTH_MODE =
  (process.env.REPLICATION_ADMIN_AUTH_MODE as ReplicationAdminAuthMode | undefined) ??
  'token_or_mtls'
const AUTH_TOKEN = process.env.REPLICATION_ADMIN_TOKEN ?? ''
const CERT_HEADER = (process.env.REPLICATION_MTLS_HEADER ?? 'x-forwarded-client-cert')
  .trim()
  .toLowerCase()
const SUBJECT_REGEX = process.env.REPLICATION_MTLS_SUBJECT_REGEX
  ? new RegExp(process.env.REPLICATION_MTLS_SUBJECT_REGEX)
  : null

const db = Database.open(DB_PATH, {
  replicationRole: 'Primary',
  replicationSidecarPath: SIDECAR_PATH,
})

const adapter: ReplicationTransportAdapter = createReplicationTransportAdapter(db)
const forwardedMatcherOptions: ReplicationForwardedMtlsMatcherOptions = {
  requireVerifyHeader: true,
  requirePeerCertificate: true,
  verifyHeaders: ['x-client-verify', 'ssl-client-verify'],
  certHeaders: [CERT_HEADER, 'x-client-cert'],
  successValues: ['success', 'verified', 'true', '1'],
}
const forwardedMatcher = createForwardedTlsMtlsMatcher(forwardedMatcherOptions)

const requireAdmin = createReplicationAdminAuthorizer<ReplicationAdminAuthRequest>({
  mode: AUTH_MODE,
  token: AUTH_TOKEN,
  mtlsMatcher: (request) => {
    const forwardedOk = forwardedMatcher(request)
    if (!forwardedOk) return false
    if (!SUBJECT_REGEX) return true
    const certValue = readHeader(request.headers ?? {}, CERT_HEADER)
    if (!certValue) return false
    return SUBJECT_REGEX.test(certValue)
  },
})

const app = express()
app.set('trust proxy', true)
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
  '/replication/promote',
  checked((req, res) => {
    ensureAdmin(req)
    const epoch = db.primaryPromoteToNextEpoch()
    res.json({ epoch, primary: db.primaryReplicationStatus() })
  }),
)

const server = app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`proxy-forwarded replication adapter listening on http://127.0.0.1:${PORT}`)
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
