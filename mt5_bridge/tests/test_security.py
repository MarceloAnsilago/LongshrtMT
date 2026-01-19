from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mt5_bridge.middleware import RequestLogMiddleware, SecurityMiddleware
from mt5_bridge.security import SecurityConfig


def build_app(config: SecurityConfig) -> FastAPI:
    app = FastAPI()
    app.add_middleware(RequestLogMiddleware, config=config)
    app.add_middleware(SecurityMiddleware, config=config)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/protected")
    def protected():
        return {"ok": True}

    return app


def test_auth_missing_key_returns_401():
    config = SecurityConfig(
        api_key="secret",
        allowed_ips=set(),
        trust_proxy=False,
        rate_limit_per_min=0,
        expose_docs=False,
    )
    client = TestClient(build_app(config))
    response = client.get("/protected")
    assert response.status_code == 401


def test_auth_wrong_key_returns_403():
    config = SecurityConfig(
        api_key="secret",
        allowed_ips=set(),
        trust_proxy=False,
        rate_limit_per_min=0,
        expose_docs=False,
    )
    client = TestClient(build_app(config))
    response = client.get("/protected", headers={"X-API-Key": "nope"})
    assert response.status_code == 403


def test_auth_correct_key_returns_200():
    config = SecurityConfig(
        api_key="secret",
        allowed_ips=set(),
        trust_proxy=False,
        rate_limit_per_min=0,
        expose_docs=False,
    )
    client = TestClient(build_app(config))
    response = client.get("/protected", headers={"X-API-Key": "secret"})
    assert response.status_code == 200


def test_health_is_public():
    config = SecurityConfig(
        api_key="secret",
        allowed_ips=set(),
        trust_proxy=False,
        rate_limit_per_min=0,
        expose_docs=False,
    )
    client = TestClient(build_app(config))
    response = client.get("/health")
    assert response.status_code == 200


def test_allowlist_blocks_non_matching_ip():
    config = SecurityConfig(
        api_key="secret",
        allowed_ips={"203.0.113.10"},
        trust_proxy=True,
        rate_limit_per_min=0,
        expose_docs=False,
    )
    client = TestClient(build_app(config))
    response = client.get("/protected", headers={"X-Forwarded-For": "198.51.100.9"})
    assert response.status_code == 403


def test_allowlist_allows_matching_ip():
    config = SecurityConfig(
        api_key="secret",
        allowed_ips={"203.0.113.10"},
        trust_proxy=True,
        rate_limit_per_min=0,
        expose_docs=False,
    )
    client = TestClient(build_app(config))
    response = client.get(
        "/protected",
        headers={"X-Forwarded-For": "203.0.113.10", "X-API-Key": "secret"},
    )
    assert response.status_code == 200


def test_rate_limit_returns_429():
    config = SecurityConfig(
        api_key="secret",
        allowed_ips=set(),
        trust_proxy=False,
        rate_limit_per_min=2,
        expose_docs=False,
    )
    client = TestClient(build_app(config))
    headers = {"X-API-Key": "secret"}
    assert client.get("/protected", headers=headers).status_code == 200
    assert client.get("/protected", headers=headers).status_code == 200
    assert client.get("/protected", headers=headers).status_code == 429
