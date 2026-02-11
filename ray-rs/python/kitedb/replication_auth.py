"""Replication admin auth helpers for host-runtime adapters."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Callable, Mapping, Optional, Pattern, Union, Literal

ReplicationAdminAuthMode = Literal[
    "none",
    "token",
    "mtls",
    "token_or_mtls",
    "token_and_mtls",
]


@dataclass(frozen=True)
class ReplicationAdminAuthConfig:
    mode: ReplicationAdminAuthMode = "none"
    token: Optional[str] = None
    mtls_header: str = "x-forwarded-client-cert"
    mtls_subject_regex: Optional[Union[str, Pattern[str]]] = None
    mtls_matcher: Optional[Callable[[Any], bool]] = None


@dataclass(frozen=True)
class AsgiMtlsMatcherOptions:
    require_peer_certificate: bool = False


_VALID_REPLICATION_ADMIN_AUTH_MODES = {
    "none",
    "token",
    "mtls",
    "token_or_mtls",
    "token_and_mtls",
}


def _normalize_regex(
    value: Optional[Union[str, Pattern[str]]],
) -> Optional[Pattern[str]]:
    if value is None:
        return None
    if isinstance(value, re.Pattern):
        return value
    return re.compile(value)


def _normalize_config(config: ReplicationAdminAuthConfig) -> ReplicationAdminAuthConfig:
    mode = (config.mode or "none").strip().lower()
    if mode not in _VALID_REPLICATION_ADMIN_AUTH_MODES:
        raise ValueError(
            f"Invalid replication admin auth mode '{mode}'; expected "
            "none|token|mtls|token_or_mtls|token_and_mtls"
        )
    token = (config.token or "").strip() or None
    if mode in {"token", "token_or_mtls", "token_and_mtls"} and not token:
        raise ValueError(
            f"replication admin auth mode '{mode}' requires a non-empty token"
        )
    mtls_header = (config.mtls_header or "").strip().lower() or "x-forwarded-client-cert"
    return ReplicationAdminAuthConfig(
        mode=mode,  # type: ignore[arg-type]
        token=token,
        mtls_header=mtls_header,
        mtls_subject_regex=_normalize_regex(config.mtls_subject_regex),
        mtls_matcher=config.mtls_matcher,
    )


def _get_header_value(headers: Any, name: str) -> Optional[str]:
    if headers is None:
        return None
    if hasattr(headers, "get"):
        direct = headers.get(name)
        if direct is None:
            direct = headers.get(name.lower())
        if isinstance(direct, str):
            trimmed = direct.strip()
            if trimmed:
                return trimmed
    if isinstance(headers, Mapping):
        for key, value in headers.items():
            if str(key).lower() != name:
                continue
            if isinstance(value, str):
                trimmed = value.strip()
                if trimmed:
                    return trimmed
    return None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"1", "true", "yes", "success"}
    return False


def _has_peer_certificate(scope: Mapping[str, Any]) -> bool:
    tls_extension = scope.get("extensions")
    if isinstance(tls_extension, Mapping):
        tls = tls_extension.get("tls")
        if isinstance(tls, Mapping):
            for key in ("client_cert", "peer_cert", "client_cert_chain"):
                value = tls.get(key)
                if value:
                    return True
    for key in ("client_cert", "peer_cert", "client_cert_chain"):
        value = scope.get(key)
        if value:
            return True
    return False


def is_asgi_tls_client_authorized(
    request: Any, options: Optional[AsgiMtlsMatcherOptions] = None
) -> bool:
    scope = getattr(request, "scope", None)
    if not isinstance(scope, Mapping):
        return False
    if _as_bool(scope.get("tls_client_authorized")) or _as_bool(
        scope.get("client_cert_verified")
    ) or _as_bool(scope.get("ssl_client_verify")):
        if options and options.require_peer_certificate:
            return _has_peer_certificate(scope)
        return True
    return False


def create_asgi_tls_mtls_matcher(
    options: Optional[AsgiMtlsMatcherOptions] = None,
) -> Callable[[Any], bool]:
    def _matcher(request: Any) -> bool:
        return is_asgi_tls_client_authorized(request, options)

    return _matcher


def is_replication_admin_authorized(
    request: Any, config: ReplicationAdminAuthConfig
) -> bool:
    normalized = _normalize_config(config)
    headers = getattr(request, "headers", None)

    token_ok = False
    if normalized.token:
        authorization = _get_header_value(headers, "authorization")
        token_ok = authorization == f"Bearer {normalized.token}"

    if normalized.mtls_matcher is not None:
        mtls_ok = bool(normalized.mtls_matcher(request))
    else:
        mtls_value = _get_header_value(headers, normalized.mtls_header)
        mtls_ok = mtls_value is not None
        pattern = normalized.mtls_subject_regex
        if mtls_ok and pattern is not None:
            mtls_ok = bool(pattern.search(mtls_value))

    if normalized.mode == "none":
        return True
    if normalized.mode == "token":
        return token_ok
    if normalized.mode == "mtls":
        return mtls_ok
    if normalized.mode == "token_or_mtls":
        return token_ok or mtls_ok
    return token_ok and mtls_ok


def authorize_replication_admin_request(
    request: Any, config: ReplicationAdminAuthConfig
) -> None:
    normalized = _normalize_config(config)
    if is_replication_admin_authorized(request, normalized):
        return
    raise PermissionError(
        f"Unauthorized: replication admin auth mode '{normalized.mode}' not satisfied"
    )


def create_replication_admin_authorizer(
    config: ReplicationAdminAuthConfig,
) -> Callable[[Any], None]:
    normalized = _normalize_config(config)

    def _authorizer(request: Any) -> None:
        authorize_replication_admin_request(request, normalized)

    return _authorizer
