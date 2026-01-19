# mt5_bridge_client/mt5client.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

try:
    import httpx
except ModuleNotFoundError:  # pragma: no cover - fallback for local shells without deps
    httpx = None
from django.conf import settings

logger = logging.getLogger(__name__)


class MT5BridgeError(Exception):
    """Errors while talking to the MT5 bridge."""


class MT5BridgeAuthError(MT5BridgeError):
    """Authentication failed when calling the MT5 bridge."""


class MT5BridgeTimeout(MT5BridgeError):
    """Timeout while calling the MT5 bridge."""


class MT5BridgeServerError(MT5BridgeError):
    """MT5 bridge returned a server error."""


class MT5BridgeRateLimit(MT5BridgeError):
    """MT5 bridge rate limit reached."""


def _get_base_url() -> str:
    base = getattr(settings, "MT5_BRIDGE_URL", "").rstrip("/")
    if not base:
        raise MT5BridgeError("MT5_BRIDGE_URL is not configured")
    return base


def _get_api_key() -> str:
    api_key = getattr(settings, "MT5_BRIDGE_API_KEY", "")
    if not api_key:
        raise MT5BridgeAuthError("MT5_BRIDGE_API_KEY is not configured")
    return api_key


def _request(method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
    if httpx is None:
        raise MT5BridgeError("httpx is not installed; install dependencies to use mt5_bridge_client")
    base_url = _get_base_url()
    url = f"{base_url}/{path.lstrip('/')}"
    url = url.rstrip("/")
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.setdefault("X-API-Key", _get_api_key())
    logger.info("MT5 bridge request %s %s", method, url)
    try:
        response = httpx.request(method, url, timeout=10.0, headers=headers, **kwargs)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        detail = exc.response.text or exc.response.reason_phrase
        if status in {401, 403}:
            raise MT5BridgeAuthError(f"MT5 bridge auth failed ({status})") from exc
        if status == 429:
            raise MT5BridgeRateLimit("MT5 bridge rate limit exceeded") from exc
        if status >= 500:
            raise MT5BridgeServerError(f"MT5 bridge error {status}: {detail}") from exc
        raise MT5BridgeError(f"MT5 bridge responded {status}: {detail}") from exc
    except httpx.TimeoutException as exc:
        raise MT5BridgeTimeout(f"MT5 bridge timed out: {exc}") from exc
    except httpx.RequestError as exc:
        raise MT5BridgeError(f"Failed to reach MT5 bridge: {exc}") from exc
    try:
        return response.json()
    except ValueError as exc:
        raise MT5BridgeError("MT5 bridge returned invalid JSON") from exc


def healthcheck() -> bool:
    data = _request("GET", "/health")
    return data.get("status") == "ok"


def get_latest_price(symbol: str) -> Optional[float]:
    data = _request("GET", f"/api/latest_price/{symbol}")
    return data.get("price")


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


def fetch_rates_range(
    symbol: str,
    timeframe: str,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    limit: int | None = None,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {"symbol": symbol, "timeframe": timeframe}
    if start:
        params["start"] = start.isoformat() if isinstance(start, datetime) else start
    if end:
        params["end"] = end.isoformat() if isinstance(end, datetime) else end
    if limit is not None:
        params["limit"] = limit
    try:
        return _request("GET", "/api/rates_range", params=params)
    except MT5BridgeError as exc:
        message = str(exc)
        if "404" in message:
            raise MT5BridgeError(
                "Endpoint rates_range não disponível no mt5_bridge"
            ) from exc
        raise


def execute_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload = {"trades": trades}
    return _request("POST", "/api/trades", json=payload).get("trades", [])


def send_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = _request("POST", "/api/trades", json={"trades": [payload]})
    trades = data.get("trades", [])
    return trades[0] if trades else {}


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
