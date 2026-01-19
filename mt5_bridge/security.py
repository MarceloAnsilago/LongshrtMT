from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import secrets
import time
from typing import Iterable, Tuple

from fastapi import Request

logger = logging.getLogger(__name__)


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    return normalized in {"1", "true", "yes", "on"}


def _parse_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class SecurityConfig:
    api_key: str | None
    allowed_ips: set[str]
    trust_proxy: bool
    rate_limit_per_min: int
    expose_docs: bool


def load_security_config() -> SecurityConfig:
    return SecurityConfig(
        api_key=os.environ.get("MT5_BRIDGE_API_KEY"),
        allowed_ips=set(_parse_csv(os.environ.get("MT5_BRIDGE_ALLOWED_IPS"))),
        trust_proxy=_parse_bool(os.environ.get("MT5_BRIDGE_TRUST_PROXY"), default=False),
        rate_limit_per_min=_parse_int(
            os.environ.get("MT5_BRIDGE_RATE_LIMIT_PER_MIN"), default=60
        ),
        expose_docs=_parse_bool(os.environ.get("MT5_BRIDGE_EXPOSE_DOCS"), default=False),
    )


def load_cors_origins() -> list[str]:
    return _parse_csv(os.environ.get("MT5_BRIDGE_CORS_ORIGINS"))


def get_client_ip(request: Request, trust_proxy: bool) -> Tuple[str, str]:
    if trust_proxy:
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            ip = forwarded_for.split(",")[0].strip()
            if ip:
                return ip, "x-forwarded-for"
    client = request.client
    if client and client.host:
        return client.host, "client"
    return "unknown", "unknown"


def is_public_path(path: str, method: str, expose_docs: bool) -> bool:
    if method.upper() == "OPTIONS":
        return True
    if path in {"/", "/health"}:
        return True
    if expose_docs and path in {"/docs", "/openapi.json", "/redoc"}:
        return True
    return False


class RateLimiter:
    def __init__(self, limit_per_minute: int):
        self.limit = max(int(limit_per_minute or 0), 0)
        self._counters: dict[str, Tuple[int, int]] = {}

    def allow(self, key: str) -> bool:
        if self.limit <= 0:
            return True
        now = int(time.time() // 60)
        window_start, count = self._counters.get(key, (now, 0))
        if window_start != now:
            window_start, count = now, 0
        count += 1
        self._counters[key] = (window_start, count)
        return count <= self.limit


def api_keys_match(candidate: str, expected: str) -> bool:
    return secrets.compare_digest(candidate, expected)


def ip_allowed(client_ip: str, allowed_ips: Iterable[str]) -> bool:
    return client_ip in set(allowed_ips)
