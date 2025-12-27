from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


class MT5BridgeError(Exception):
    """Erro ocorrendo ao falar com o bridge do MT5."""


def _get_base_url() -> str:
    url = getattr(settings, "MT5_BRIDGE_URL", None)
    if not url:
        raise ImproperlyConfigured("MT5_BRIDGE_URL não configurado")
    return url.rstrip("/")


def _request(method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
    base_url = _get_base_url()
    url = f"{base_url}/{path.lstrip('/')}"
    try:
        response = httpx.request(method, url, timeout=10, **kwargs)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text or exc.response.reason_phrase
        raise MT5BridgeError(f"MT5 bridge respondeu {exc.response.status_code}: {detail}") from exc
    except httpx.RequestError as exc:
        raise MT5BridgeError(f"Falha ao conectar ao MT5 bridge: {exc}") from exc
    return response.json()


def get_latest_price(symbol: str) -> Optional[float]:
    """
    Obtém o último preço exposto pelo bridge.
    """
    data = _request("GET", f"/api/latest_price/{symbol}")
    return data.get("price")


def fetch_rates(symbol: str, timeframe: int, count: int) -> List[Dict[str, Any]]:
    payload = {"symbol": symbol, "timeframe": timeframe, "count": count}
    data = _request("POST", "/api/rates", json=payload)
    return data.get("rates", [])


def fetch_rates_range(symbol: str, timeframe: int, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    payload = {
        "symbol": symbol,
        "timeframe": timeframe,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    data = _request("POST", "/api/rates/range", json=payload)
    return data.get("rates", [])


def bulk_update_quotes(symbols: Optional[List[str]] = None) -> Dict[str, Any]:
    payload = {"symbols": symbols}
    return _request("POST", "/api/bulk_update_quotes", json=payload)
