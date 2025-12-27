from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import MetaTrader5 as mt5

logger = logging.getLogger(__name__)


def _ensure_symbol(symbol: str) -> bool:
    """
    Garante que o símbolo esteja visível/selecionado no MT5.
    """
    info = mt5.symbol_info(symbol)
    if info is None:
        return False
    if not info.visible:
        if not mt5.symbol_select(symbol, True):
            return False
    return True


def _format_mt5_error() -> str:
    err = mt5.last_error()
    if not err or err[0] == 0:
        return "unknown"
    return f"{err[1]} ({err[0]})"


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    dtype = getattr(row, "dtype", None)
    if dtype and hasattr(dtype, "names"):
        return {name: row[i] for i, name in enumerate(dtype.names)}
    return {str(i): value for i, value in enumerate(row)}


def _normalize_rates(raw: List[Any]) -> List[Dict[str, Any]]:
    rates: List[Dict[str, Any]] = []
    for row in raw:
        rates.append(_row_to_dict(row))
    return sorted(rates, key=lambda rate: rate.get("time", 0))


def get_latest_price(symbol: str) -> float | None:
    """
    Retorna o último preço (last) ou bid do símbolo.
    """
    if not _ensure_symbol(symbol):
        return None

    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None

    price = tick.last if tick.last > 0 else tick.bid
    if price <= 0:
        return None

    return float(price)


def fetch_rates(symbol: str, timeframe: int, count: int) -> List[Dict[str, Any]]:
    """
    Retorna barras geradas por `mt5.copy_rates_from`.
    """
    if not _ensure_symbol(symbol):
        raise RuntimeError(f"Símbolo {symbol} indisponível no MT5")

    now = datetime.now()
    raw = mt5.copy_rates_from(symbol, timeframe, now, count)
    if raw is None:
        error_detail = _format_mt5_error()
        logger.error("MT5.copy_rates_from falhou: %s", error_detail)
        raise RuntimeError(error_detail)
    return _normalize_rates(list(raw))


def fetch_rates_range(symbol: str, timeframe: int, start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    """
    Retorna barras via `mt5.copy_rates_range`.
    """
    if not _ensure_symbol(symbol):
        raise RuntimeError(f"Símbolo {symbol} indisponível no MT5")

    raw = mt5.copy_rates_range(symbol, timeframe, start_dt, end_dt)
    if raw is None:
        error_detail = _format_mt5_error()
        logger.error("MT5.copy_rates_range falhou: %s", error_detail)
        raise RuntimeError(error_detail)
    return _normalize_rates(list(raw))


def bulk_update_quotes(symbols: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Versão simplificada para teste: retorna o último preço de cada símbolo solicitado.
    """
    result: Dict[str, Any] = {"symbols": []}

    if symbols is None:
        all_symbols = mt5.symbols_get()
        symbols = [s.name for s in all_symbols]

    for sym in symbols:
        price = get_latest_price(sym)
        result["symbols"].append(
            {
                "symbol": sym,
                "price": price,
                "ok": price is not None,
            }
        )

    return result
