# mt5_bridge_client/mt5client.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

import httpx
from django.conf import settings

logger = logging.getLogger(__name__)


class MT5BridgeError(Exception):
    """Errors while talking to the MT5 bridge."""


def _get_base_url() -> str:
    base = getattr(settings, "MT5_BRIDGE_URL", "").rstrip("/")
    if not base:
        raise MT5BridgeError("MT5_BRIDGE_URL is not configured")
    return base


def _request(method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
    base_url = _get_base_url()
    url = f"{base_url}/{path.lstrip('/')}"
    url = url.rstrip("/")
    logger.info("MT5 bridge request %s %s", method, url)
    try:
        response = httpx.request(method, url, timeout=20.0, **kwargs)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text or exc.response.reason_phrase
        raise MT5BridgeError(f"MT5 bridge responded {exc.response.status_code}: {detail}") from exc
    except httpx.RequestError as exc:
        raise MT5BridgeError(f"Failed to reach MT5 bridge: {exc}") from exc
    return response.json()


def fetch_last_bar_d1(symbol: str) -> Optional[Dict[str, Any]]:
    payload = {"symbol": symbol, "timeframe": "D1", "count": 1}
    return _request("POST", "/api/rates", json=payload).get("rates", [None])[0]


def fetch_last_close_d1(symbol: str) -> Optional[float]:
    bar = fetch_last_bar_d1(symbol)
    close = bar.get("close") if bar else None
    return float(close) if close is not None else None


def fetch_rates(symbol: str, timeframe: str = "D1", count: int = 1) -> list[Dict[str, Any]]:
    payload = {"symbol": symbol, "timeframe": timeframe, "count": count}
    return _request("POST", "/api/rates", json=payload).get("rates", [])


def execute_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload = {"trades": trades}
    return _request("POST", "/api/trades", json=payload).get("trades", [])


def explain_close(identifier: int, from_dt: datetime, to_dt: datetime) -> Dict[str, Any]:
    payload = {
        "identifier": identifier,
        "from_dt": from_dt.isoformat(),
        "to_dt": to_dt.isoformat(),
    }
    return _request("POST", "/api/history/explain_close", json=payload)


def fetch_positions() -> list[dict[str, Any]]:
    return _request("GET", "/api/positions").get("positions", [])


def fetch_history_deals(from_dt: datetime, to_dt: datetime) -> list[dict[str, Any]]:
    payload = {
        "from_dt": from_dt.isoformat(),
        "to_dt": to_dt.isoformat(),
    }
    return _request("POST", "/api/history/deals", json=payload).get("deals", [])


def fetch_account_info() -> Dict[str, Any]:
    return _request("GET", "/api/account_info")
