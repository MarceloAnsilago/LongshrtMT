from __future__ import annotations

import logging
import time

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from .security import (
    RateLimiter,
    SecurityConfig,
    api_keys_match,
    get_client_ip,
    ip_allowed,
    is_public_path,
)

logger = logging.getLogger(__name__)


class RequestLogMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, config: SecurityConfig):
        super().__init__(app)
        self.config = config

    async def dispatch(self, request: Request, call_next):
        start = time.perf_counter()
        client_ip, _ = get_client_ip(request, self.config.trust_proxy)
        try:
            response = await call_next(request)
        except Exception:
            logger.exception(
                "Request failed method=%s path=%s client_ip=%s",
                request.method,
                request.url.path,
                client_ip,
            )
            raise
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "Request method=%s path=%s status=%s elapsed_ms=%.2f client_ip=%s",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
            client_ip,
        )
        return response


class SecurityMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, config: SecurityConfig):
        super().__init__(app)
        self.config = config
        self.rate_limiter = RateLimiter(config.rate_limit_per_min)

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        method = request.method
        client_ip, ip_source = get_client_ip(request, self.config.trust_proxy)

        if self.config.allowed_ips:
            if not ip_allowed(client_ip, self.config.allowed_ips):
                logger.warning(
                    "Blocked by allowlist client_ip=%s source=%s path=%s",
                    client_ip,
                    ip_source,
                    path,
                )
                return JSONResponse(
                    status_code=403,
                    content={"detail": "IP not allowed"},
                )

        if self.config.rate_limit_per_min > 0:
            if not self.rate_limiter.allow(client_ip):
                logger.warning(
                    "Rate limit exceeded client_ip=%s path=%s",
                    client_ip,
                    path,
                )
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Rate limit exceeded"},
                )

        if is_public_path(path, method, self.config.expose_docs):
            return await call_next(request)

        if not self.config.api_key:
            logger.error("API key not configured; denying request path=%s", path)
            return JSONResponse(
                status_code=500,
                content={"detail": "API key not configured"},
            )

        provided_key = request.headers.get("x-api-key")
        if not provided_key:
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing API key"},
            )
        if not api_keys_match(provided_key, self.config.api_key):
            return JSONResponse(
                status_code=403,
                content={"detail": "Invalid API key"},
            )

        return await call_next(request)
