"""
Host-runtime replication HTTP adapter template (Python + FastAPI).

Purpose:
- expose replication admin/transport endpoints outside playground runtime
- reuse kitedb host-runtime APIs directly
- include token/mTLS auth parity helper for host adapters

Run:
  pip install fastapi uvicorn kitedb
  export REPLICATION_ADMIN_TOKEN=change-me
  export REPLICATION_ADMIN_AUTH_MODE=token_or_mtls
  uvicorn replication_adapter_python_fastapi:app --host 0.0.0.0 --port 8080
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from kitedb import (
  AsgiMtlsMatcherOptions,
  Database,
  OpenOptions,
  ReplicationAdminAuthConfig,
  collect_replication_log_transport_json,
  collect_replication_metrics_otel_json,
  collect_replication_metrics_prometheus,
  collect_replication_snapshot_transport_json,
  create_asgi_tls_mtls_matcher,
  create_replication_admin_authorizer,
)


@dataclass(frozen=True)
class Settings:
  db_path: str = os.environ.get("KITEDB_PATH", "cluster-primary.kitedb")
  replication_admin_auth_mode: str = os.environ.get(
    "REPLICATION_ADMIN_AUTH_MODE", "token_or_mtls"
  )
  replication_admin_token: str = os.environ.get("REPLICATION_ADMIN_TOKEN", "")
  replication_mtls_header: str = os.environ.get(
    "REPLICATION_MTLS_HEADER", "x-forwarded-client-cert"
  )
  replication_mtls_subject_regex: str = os.environ.get(
    "REPLICATION_MTLS_SUBJECT_REGEX", ""
  )


SETTINGS = Settings()
DB = Database(
  SETTINGS.db_path,
  OpenOptions(
    replication_role="primary",
    replication_sidecar_path=os.environ.get(
      "KITEDB_REPLICATION_SIDECAR",
      "cluster-primary.sidecar",
    ),
  ),
)

app = FastAPI(title="kitedb-replication-adapter")


_ADMIN_AUTH = ReplicationAdminAuthConfig(
  mode=SETTINGS.replication_admin_auth_mode,  # type: ignore[arg-type]
  token=SETTINGS.replication_admin_token,
  mtls_header=SETTINGS.replication_mtls_header,
  mtls_subject_regex=SETTINGS.replication_mtls_subject_regex or None,
  mtls_matcher=create_asgi_tls_mtls_matcher(
    AsgiMtlsMatcherOptions(require_peer_certificate=False)
  ),
)
_REQUIRE_ADMIN = create_replication_admin_authorizer(_ADMIN_AUTH)


def _require_admin(request: Request) -> None:
  try:
    _REQUIRE_ADMIN(request)
  except PermissionError as error:
    raise HTTPException(status_code=401, detail=str(error)) from error


def _json_loads(raw: str, label: str) -> Any:
  try:
    return json.loads(raw)
  except json.JSONDecodeError as error:
    raise HTTPException(
      status_code=500,
      detail=f"invalid {label} payload: {error}",
    ) from error


class PullRequest(BaseModel):
  max_frames: int = 256


@app.get("/replication/status")
def replication_status() -> dict[str, Any]:
  return {
    "primary": DB.primary_replication_status(),
    "replica": DB.replica_replication_status(),
  }


@app.get("/replication/metrics/prometheus", response_class=PlainTextResponse)
def replication_metrics_prometheus(_: None = Depends(_require_admin)) -> str:
  return collect_replication_metrics_prometheus(DB)


@app.get("/replication/metrics/otel-json")
def replication_metrics_otel_json(_: None = Depends(_require_admin)) -> Any:
  return _json_loads(collect_replication_metrics_otel_json(DB), "otel-json")


@app.get("/replication/transport/snapshot")
def replication_snapshot_transport(
  include_data: bool = Query(default=False),
  _: None = Depends(_require_admin),
) -> Any:
  raw = collect_replication_snapshot_transport_json(DB, include_data=include_data)
  return _json_loads(raw, "snapshot transport")


@app.get("/replication/transport/log")
def replication_log_transport(
  cursor: Optional[str] = Query(default=None),
  max_frames: int = Query(default=128, ge=1, le=10_000),
  max_bytes: int = Query(default=1_048_576, ge=1, le=32 * 1024 * 1024),
  include_payload: bool = Query(default=True),
  _: None = Depends(_require_admin),
) -> Any:
  raw = collect_replication_log_transport_json(
    DB,
    cursor=cursor,
    max_frames=max_frames,
    max_bytes=max_bytes,
    include_payload=include_payload,
  )
  return _json_loads(raw, "log transport")


@app.post("/replication/pull")
def replication_pull(body: PullRequest, _: None = Depends(_require_admin)) -> dict[str, Any]:
  applied = DB.replica_catch_up_once(body.max_frames)
  return {
    "applied_frames": applied,
    "replica": DB.replica_replication_status(),
  }


@app.post("/replication/reseed")
def replication_reseed(_: None = Depends(_require_admin)) -> dict[str, Any]:
  DB.replica_reseed_from_snapshot()
  return {"replica": DB.replica_replication_status()}


@app.post("/replication/promote")
def replication_promote(_: None = Depends(_require_admin)) -> dict[str, Any]:
  epoch = DB.primary_promote_to_next_epoch()
  return {"epoch": epoch, "primary": DB.primary_replication_status()}
