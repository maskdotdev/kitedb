/**
 * API Client
 *
 * Fetch wrappers for all backend endpoints.
 */

import type {
  StatusResponse,
  StatsResponse,
  GraphNetwork,
  PathResponse,
  ImpactResponse,
  ApiResult,
  ReplicationStatusResponse,
  ReplicationSnapshotResponse,
  ReplicationLogResponse,
  ReplicationPullResponse,
  ReplicationReseedResponse,
  ReplicationPromoteResponse,
} from "./types.ts";

const API_BASE = "/api";

export interface DbOpenOptions {
  readOnly?: boolean
  createIfMissing?: boolean
  mvcc?: boolean
  mvccGcIntervalMs?: number
  mvccRetentionMs?: number
  mvccMaxChainDepth?: number
  syncMode?: "Full" | "Normal" | "Off"
  groupCommitEnabled?: boolean
  groupCommitWindowMs?: number
  walSizeMb?: number
  checkpointThreshold?: number
  replicationRole?: "disabled" | "primary" | "replica"
  replicationSidecarPath?: string
  replicationSourceDbPath?: string
  replicationSourceSidecarPath?: string
  replicationSegmentMaxBytes?: number
  replicationRetentionMinEntries?: number
  replicationRetentionMinMs?: number
}

// ============================================================================
// Helper
// ============================================================================

async function fetchJson<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${url}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });
  
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  
  return response.json();
}

async function fetchText(url: string, options?: RequestInit): Promise<string> {
  const response = await fetch(`${API_BASE}${url}`, {
    ...options,
    headers: {
      ...options?.headers,
    },
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }

  return response.text();
}

function withAuthHeader(token?: string): HeadersInit | undefined {
  if (!token || token.trim() === "") {
    return undefined;
  }
  return { Authorization: `Bearer ${token}` };
}

// ============================================================================
// Database Management
// ============================================================================

export async function getStatus(): Promise<StatusResponse> {
  return fetchJson<StatusResponse>("/status");
}

export async function openDatabase(path: string, options?: DbOpenOptions): Promise<ApiResult> {
  return fetchJson<ApiResult>("/db/open", {
    method: "POST",
    body: JSON.stringify({
      path,
      ...(options ? { options } : {}),
    }),
  });
}

export async function uploadDatabase(file: File): Promise<ApiResult> {
  const formData = new FormData();
  formData.append("file", file);
  
  const response = await fetch(`${API_BASE}/db/upload`, {
    method: "POST",
    body: formData,
  });
  
  return response.json();
}

export async function createDemo(): Promise<ApiResult> {
  return fetchJson<ApiResult>("/db/demo", {
    method: "POST",
  });
}

export async function closeDatabase(): Promise<ApiResult> {
  return fetchJson<ApiResult>("/db/close", {
    method: "POST",
  });
}

// ============================================================================
// Replication
// ============================================================================

export interface ReplicationAuthOptions {
  adminToken?: string
}

export interface ReplicationSnapshotOptions extends ReplicationAuthOptions {
  includeData?: boolean
}

export interface ReplicationLogOptions extends ReplicationAuthOptions {
  cursor?: string
  maxBytes?: number
  maxFrames?: number
  includePayload?: boolean
}

export interface ReplicationPullOptions extends ReplicationAuthOptions {
  maxFrames?: number
}

export async function getReplicationStatus(): Promise<ReplicationStatusResponse> {
  return fetchJson<ReplicationStatusResponse>("/replication/status");
}

export async function getReplicationMetricsPrometheus(
  options?: ReplicationAuthOptions,
): Promise<string> {
  return fetchText("/replication/metrics", {
    headers: withAuthHeader(options?.adminToken),
  });
}

export async function getReplicationSnapshotLatest(
  options?: ReplicationSnapshotOptions,
): Promise<ReplicationSnapshotResponse> {
  const params = new URLSearchParams();
  if (typeof options?.includeData === "boolean") {
    params.set("includeData", options.includeData ? "true" : "false");
  }
  const query = params.size > 0 ? `?${params.toString()}` : "";

  return fetchJson<ReplicationSnapshotResponse>(`/replication/snapshot/latest${query}`, {
    headers: withAuthHeader(options?.adminToken),
  });
}

export async function getReplicationLog(
  options?: ReplicationLogOptions,
): Promise<ReplicationLogResponse> {
  const params = new URLSearchParams();
  if (options?.cursor) {
    params.set("cursor", options.cursor);
  }
  if (typeof options?.maxBytes === "number") {
    params.set("maxBytes", String(options.maxBytes));
  }
  if (typeof options?.maxFrames === "number") {
    params.set("maxFrames", String(options.maxFrames));
  }
  if (typeof options?.includePayload === "boolean") {
    params.set("includePayload", options.includePayload ? "true" : "false");
  }
  const query = params.size > 0 ? `?${params.toString()}` : "";

  return fetchJson<ReplicationLogResponse>(`/replication/log${query}`, {
    headers: withAuthHeader(options?.adminToken),
  });
}

export async function pullReplicaOnce(
  options?: ReplicationPullOptions,
): Promise<ReplicationPullResponse> {
  return fetchJson<ReplicationPullResponse>("/replication/pull", {
    method: "POST",
    headers: withAuthHeader(options?.adminToken),
    body: JSON.stringify(
      typeof options?.maxFrames === "number"
        ? { maxFrames: options.maxFrames }
        : {},
    ),
  });
}

export async function reseedReplica(
  options?: ReplicationAuthOptions,
): Promise<ReplicationReseedResponse> {
  return fetchJson<ReplicationReseedResponse>("/replication/reseed", {
    method: "POST",
    headers: withAuthHeader(options?.adminToken),
  });
}

export async function promotePrimary(
  options?: ReplicationAuthOptions,
): Promise<ReplicationPromoteResponse> {
  return fetchJson<ReplicationPromoteResponse>("/replication/promote", {
    method: "POST",
    headers: withAuthHeader(options?.adminToken),
  });
}

// ============================================================================
// Stats
// ============================================================================

export async function getStats(): Promise<StatsResponse> {
  return fetchJson<StatsResponse>("/stats");
}

// ============================================================================
// Graph Network
// ============================================================================

export async function getGraphNetwork(): Promise<GraphNetwork> {
  return fetchJson<GraphNetwork>("/graph/network");
}

// ============================================================================
// Path Finding
// ============================================================================

export async function findPath(startKey: string, endKey: string): Promise<PathResponse> {
  return fetchJson<PathResponse>("/graph/path", {
    method: "POST",
    body: JSON.stringify({ startKey, endKey }),
  });
}

// ============================================================================
// Impact Analysis
// ============================================================================

export async function analyzeImpact(nodeKey: string): Promise<ImpactResponse> {
  return fetchJson<ImpactResponse>("/graph/impact", {
    method: "POST",
    body: JSON.stringify({ nodeKey }),
  });
}
