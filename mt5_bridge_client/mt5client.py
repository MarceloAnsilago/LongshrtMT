# mt5_bridge_client/mt5client.py
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import httpx
from django.conf import settings

logger = logging.getLogger(__name__)


class MT5BridgeError(Exception):
    """Erros ao falar com a MT5 bridge."""


def _get_base_url() -> str:
    base = getattr(settings, "MT5_BRIDGE_URL", "").rstrip("/")
    if not base:
        raise MT5BridgeError("MT5_BRIDGE_URL não configurada no settings.")
    return base


def fetch_last_bar_d1(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Busca a última barra D1 para o símbolo informado via /api/rates da bridge.
    """
    base_url = _get_base_url()
    url = f"{base_url}/api/rates"
    payload = {"symbol": symbol, "timeframe": "D1", "count": 1}

    logger.info("Chamando MT5 bridge %s com payload %s", url, payload)

    try:
        resp = httpx.post(url, json=payload, timeout=10.0)
    except httpx.RequestError as exc:
        logger.error("Falha ao conectar ao MT5 bridge: %s", exc)
        raise MT5BridgeError(f"Falha ao conectar ao MT5 bridge: {exc}") from exc

    if resp.status_code == 200:
        try:
            data = resp.json()
        except ValueError as exc:
            logger.error("Resposta inválida do MT5 bridge (JSON): %s", resp.text)
            raise MT5BridgeError("Resposta inválida do MT5 bridge (JSON).") from exc

        rates = data.get("rates", [])
        if not rates:
            logger.warning("Nenhuma barra retornada para %s (D1).", symbol)
            return None

        bar = rates[0]
        logger.info("Última barra D1 de %s: %s", symbol, bar)
        return bar

    if resp.status_code == 422:
        try:
            detail = resp.json().get("detail")
        except Exception:
            detail = resp.text
        msg = f"MT5 retornou erro 422 para {symbol}: {detail}"
        logger.error(msg)
        raise MT5BridgeError(msg)

    if resp.status_code == 400:
        try:
            detail = resp.json().get("detail")
        except Exception:
            detail = resp.text
        msg = f"Parâmetros inválidos ao chamar MT5 bridge para {symbol}: {detail}"
        logger.error(msg)
        raise MT5BridgeError(msg)

    msg = f"Erro inesperado ao chamar MT5 bridge ({resp.status_code}): {resp.text}"
    logger.error(msg)
    raise MT5BridgeError(msg)


def fetch_last_close_d1(symbol: str) -> Optional[float]:
    bar = fetch_last_bar_d1(symbol)
    if not bar:
        return None
    close = bar.get("close")
    return float(close) if close is not None else None


def execute_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base_url = _get_base_url()
    url = f"{base_url}/api/trades"
    payload = {"trades": trades}

    logger.info("Executando ordens no MT5 bridge %s", url)

    try:
        resp = httpx.post(url, json=payload, timeout=20.0)
    except httpx.RequestError as exc:
        logger.error("Falha ao conectar ao MT5 bridge para ordens: %s", exc)
        raise MT5BridgeError(f"Falha ao conectar ao MT5 bridge: {exc}") from exc

    if resp.status_code in {400, 422}:
        try:
            detail = resp.json().get("detail")
        except Exception:
            detail = resp.text
        msg = f"MT5 bridge respondeu {resp.status_code}: {detail}"
        logger.error(msg)
        raise MT5BridgeError(msg)

    if resp.status_code != 200:
        msg = f"Erro inesperado ao chamar MT5 bridge ({resp.status_code}): {resp.text}"
        logger.error(msg)
        raise MT5BridgeError(msg)

    try:
        data = resp.json()
    except ValueError as exc:
        logger.error("Resposta inválida do MT5 bridge (JSON): %s", resp.text)
        raise MT5BridgeError("Resposta inválida do MT5 bridge (JSON).") from exc

    trades_data = data.get("trades")
    if trades_data is None:
        msg = "Resposta do MT5 bridge não contém dados de ordens."
        logger.error(msg)
        raise MT5BridgeError(msg)

    return trades_data
