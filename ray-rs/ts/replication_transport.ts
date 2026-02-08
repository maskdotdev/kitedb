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

export type ReplicationAdminAuthMode = 'none' | 'token' | 'mtls' | 'token_or_mtls' | 'token_and_mtls'

export interface ReplicationAdminAuthRequest {
  headers?: Record<string, string | undefined> | null
}

export interface ReplicationAdminAuthConfig<
  TRequest extends ReplicationAdminAuthRequest = ReplicationAdminAuthRequest,
> {
  mode?: ReplicationAdminAuthMode
  token?: string | null
  mtlsHeader?: string
  mtlsSubjectRegex?: RegExp | null
  mtlsMatcher?: (request: TRequest) => boolean
}

export interface ReplicationNodeTlsLikeSocket {
  authorized?: boolean | null
  getPeerCertificate?: () => unknown
}

export interface ReplicationNodeTlsLikeRequest extends ReplicationAdminAuthRequest {
  socket?: ReplicationNodeTlsLikeSocket | null
  client?: ReplicationNodeTlsLikeSocket | null
  raw?: { socket?: ReplicationNodeTlsLikeSocket | null } | null
  req?: { socket?: ReplicationNodeTlsLikeSocket | null } | null
}

export interface ReplicationNodeMtlsMatcherOptions {
  requirePeerCertificate?: boolean
}

export interface ReplicationForwardedMtlsMatcherOptions {
  requirePeerCertificate?: boolean
  requireVerifyHeader?: boolean
  verifyHeaders?: string[]
  certHeaders?: string[]
  successValues?: string[]
}

const REPLICATION_ADMIN_AUTH_MODES = new Set<ReplicationAdminAuthMode>([
  'none',
  'token',
  'mtls',
  'token_or_mtls',
  'token_and_mtls',
])

const DEFAULT_FORWARDED_VERIFY_HEADERS = ['x-client-verify', 'ssl-client-verify']
const DEFAULT_FORWARDED_CERT_HEADERS = ['x-forwarded-client-cert', 'x-client-cert']
const DEFAULT_FORWARDED_SUCCESS_VALUES = ['success', 'successful', 'true', '1', 'yes', 'verified', 'ok']

function hasPeerCertificate(socket: ReplicationNodeTlsLikeSocket): boolean {
  if (!socket.getPeerCertificate) return false
  try {
    const certificate = socket.getPeerCertificate()
    if (!certificate || typeof certificate !== 'object') return false
    return Object.keys(certificate as Record<string, unknown>).length > 0
  } catch {
    return false
  }
}

function isSocketAuthorized(
  socket: ReplicationNodeTlsLikeSocket | null | undefined,
  options: Required<ReplicationNodeMtlsMatcherOptions>,
): boolean {
  if (!socket || socket.authorized !== true) return false
  if (!options.requirePeerCertificate) return true
  return hasPeerCertificate(socket)
}

export function isNodeTlsClientAuthorized(
  request: ReplicationNodeTlsLikeRequest,
  options: ReplicationNodeMtlsMatcherOptions = {},
): boolean {
  const resolved: Required<ReplicationNodeMtlsMatcherOptions> = {
    requirePeerCertificate: options.requirePeerCertificate ?? false,
  }
  return (
    isSocketAuthorized(request.socket, resolved) ||
    isSocketAuthorized(request.client, resolved) ||
    isSocketAuthorized(request.raw?.socket, resolved) ||
    isSocketAuthorized(request.req?.socket, resolved)
  )
}

export function createNodeTlsMtlsMatcher(
  options: ReplicationNodeMtlsMatcherOptions = {},
): (request: ReplicationNodeTlsLikeRequest) => boolean {
  return (request: ReplicationNodeTlsLikeRequest): boolean => isNodeTlsClientAuthorized(request, options)
}

function normalizeHeaderNames(headers: string[] | undefined, fallback: string[]): string[] {
  const names = (headers ?? fallback)
    .map((name) => name.trim().toLowerCase())
    .filter((name) => name.length > 0)
  if (names.length > 0) return names
  return fallback
}

function normalizeHeaderValues(values: string[] | undefined, fallback: string[]): Set<string> {
  const normalized = (values ?? fallback)
    .map((value) => value.trim().toLowerCase())
    .filter((value) => value.length > 0)
  if (normalized.length > 0) return new Set(normalized)
  return new Set(fallback)
}

export function isForwardedTlsClientAuthorized(
  request: ReplicationAdminAuthRequest,
  options: ReplicationForwardedMtlsMatcherOptions = {},
): boolean {
  const verifyHeaders = normalizeHeaderNames(options.verifyHeaders, DEFAULT_FORWARDED_VERIFY_HEADERS)
  const certHeaders = normalizeHeaderNames(options.certHeaders, DEFAULT_FORWARDED_CERT_HEADERS)
  const successValues = normalizeHeaderValues(options.successValues, DEFAULT_FORWARDED_SUCCESS_VALUES)
  const requireVerifyHeader = options.requireVerifyHeader ?? true
  const requirePeerCertificate = options.requirePeerCertificate ?? false

  const verifyValues: string[] = []
  for (const header of verifyHeaders) {
    const value = getHeaderValue(request, header)
    if (value) verifyValues.push(value.toLowerCase())
  }
  const verifyOk = verifyValues.length > 0
    ? verifyValues.some((value) => successValues.has(value))
    : !requireVerifyHeader
  if (!verifyOk) return false

  if (!requirePeerCertificate) return true
  for (const header of certHeaders) {
    if (getHeaderValue(request, header)) return true
  }
  return false
}

export function createForwardedTlsMtlsMatcher(
  options: ReplicationForwardedMtlsMatcherOptions = {},
): (request: ReplicationAdminAuthRequest) => boolean {
  return (request: ReplicationAdminAuthRequest): boolean =>
    isForwardedTlsClientAuthorized(request, options)
}

type NormalizedReplicationAdminAuthConfig<TRequest extends ReplicationAdminAuthRequest = ReplicationAdminAuthRequest> =
  {
    mode: ReplicationAdminAuthMode
    token: string | null
    mtlsHeader: string
    mtlsSubjectRegex: RegExp | null
    mtlsMatcher: ((request: TRequest) => boolean) | null
  }

function normalizeReplicationAdminAuthConfig<
  TRequest extends ReplicationAdminAuthRequest = ReplicationAdminAuthRequest,
>(config: ReplicationAdminAuthConfig<TRequest>): NormalizedReplicationAdminAuthConfig<TRequest> {
  const modeRaw = config.mode ?? 'none'
  if (!REPLICATION_ADMIN_AUTH_MODES.has(modeRaw)) {
    throw new Error(
      `Invalid replication admin auth mode '${String(modeRaw)}'; expected none|token|mtls|token_or_mtls|token_and_mtls`,
    )
  }
  const token = config.token?.trim() || null
  if ((modeRaw === 'token' || modeRaw === 'token_or_mtls' || modeRaw === 'token_and_mtls') && !token) {
    throw new Error(`replication admin auth mode '${modeRaw}' requires a non-empty token`)
  }
  const mtlsHeaderRaw = config.mtlsHeader?.trim().toLowerCase()
  const mtlsHeader = mtlsHeaderRaw && mtlsHeaderRaw.length > 0 ? mtlsHeaderRaw : 'x-forwarded-client-cert'
  return {
    mode: modeRaw,
    token,
    mtlsHeader,
    mtlsSubjectRegex: config.mtlsSubjectRegex ?? null,
    mtlsMatcher: config.mtlsMatcher ?? null,
  }
}

function getHeaderValue(request: ReplicationAdminAuthRequest, name: string): string | null {
  const headers = request.headers
  if (!headers) return null
  const direct = headers[name]
  if (typeof direct === 'string' && direct.trim().length > 0) {
    return direct.trim()
  }
  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() !== name) continue
    if (typeof value !== 'string') continue
    const trimmed = value.trim()
    if (trimmed.length > 0) return trimmed
  }
  return null
}

function isTokenMatch(request: ReplicationAdminAuthRequest, token: string | null): boolean {
  if (!token) return false
  const authorization = getHeaderValue(request, 'authorization')
  if (!authorization) return false
  return authorization === `Bearer ${token}`
}

function isMtlsMatch<TRequest extends ReplicationAdminAuthRequest = ReplicationAdminAuthRequest>(
  request: TRequest,
  config: NormalizedReplicationAdminAuthConfig<TRequest>,
): boolean {
  if (config.mtlsMatcher) {
    return config.mtlsMatcher(request)
  }
  const certValue = getHeaderValue(request, config.mtlsHeader)
  if (!certValue) return false
  if (!config.mtlsSubjectRegex) return true
  return config.mtlsSubjectRegex.test(certValue)
}

function isAuthorizedWithNormalized<TRequest extends ReplicationAdminAuthRequest = ReplicationAdminAuthRequest>(
  request: TRequest,
  config: NormalizedReplicationAdminAuthConfig<TRequest>,
): boolean {
  const tokenOk = isTokenMatch(request, config.token)
  const mtlsOk = isMtlsMatch(request, config)

  switch (config.mode) {
    case 'none':
      return true
    case 'token':
      return tokenOk
    case 'mtls':
      return mtlsOk
    case 'token_or_mtls':
      return tokenOk || mtlsOk
    case 'token_and_mtls':
      return tokenOk && mtlsOk
  }
}

export function isReplicationAdminAuthorized<
  TRequest extends ReplicationAdminAuthRequest = ReplicationAdminAuthRequest,
>(request: TRequest, config: ReplicationAdminAuthConfig<TRequest>): boolean {
  const normalized = normalizeReplicationAdminAuthConfig(config)
  return isAuthorizedWithNormalized(request, normalized)
}

export function authorizeReplicationAdminRequest<
  TRequest extends ReplicationAdminAuthRequest = ReplicationAdminAuthRequest,
>(request: TRequest, config: ReplicationAdminAuthConfig<TRequest>): void {
  const normalized = normalizeReplicationAdminAuthConfig(config)
  if (isAuthorizedWithNormalized(request, normalized)) {
    return
  }
  throw new Error(`Unauthorized: replication admin auth mode '${normalized.mode}' not satisfied`)
}

export function createReplicationAdminAuthorizer<
  TRequest extends ReplicationAdminAuthRequest = ReplicationAdminAuthRequest,
>(config: ReplicationAdminAuthConfig<TRequest>): (request: TRequest) => void {
  const normalized = normalizeReplicationAdminAuthConfig(config)
  return (request: TRequest): void => {
    if (isAuthorizedWithNormalized(request, normalized)) {
      return
    }
    throw new Error(`Unauthorized: replication admin auth mode '${normalized.mode}' not satisfied`)
  }
}

function parseJson<T>(raw: string, label: string): T {
  try {
    return JSON.parse(raw) as T
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`Failed to parse ${label}: ${message}`)
  }
}

export function readReplicationSnapshotTransport(db: Database, includeData = false): ReplicationSnapshotTransport {
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
