from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Literal
import logging
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import MetaTrader5 as mt5  # para usar as constantes de timeframe

from .mt5_session import init_mt5
from . import quotes_core

logger = logging.getLogger(__name__)

app = FastAPI(title="MT5 Bridge", version="0.1.0")

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

TRADE_MAGIC = _env_int("MT5_TRADE_MAGIC", 741853)
TRADE_DEFAULT_DEVIATION = _env_int("MT5_TRADE_DEVIATION", 20)
TRADE_COMMENT = os.environ.get("MT5_TRADE_COMMENT", "LongShort")


# ------------------------------------------------------------
# Eventos de ciclo de vida
# ------------------------------------------------------------
@app.on_event("startup")
def startup_event():
    # inicializa conexão com o MT5 quando o servidor sobe
    init_mt5()


# ------------------------------------------------------------
# Mapeamento de timeframes
# ------------------------------------------------------------
TIMEFRAME_MAP_STR: Dict[str, int] = {
    "M1": mt5.TIMEFRAME_M1,
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "D1": mt5.TIMEFRAME_D1,
}

TIMEFRAME_MAP_INT: Dict[int, int] = {
    1: mt5.TIMEFRAME_M1,
    5: mt5.TIMEFRAME_M5,
    15: mt5.TIMEFRAME_M15,
    30: mt5.TIMEFRAME_M30,
    60: mt5.TIMEFRAME_H1,
    1440: mt5.TIMEFRAME_D1,
}


TimeframeLiteral = Literal["M1", "M5", "M15", "M30", "H1", "D1"]


def _resolve_timeframe(tf: Union[int, str]) -> int:
    if isinstance(tf, int):
        if tf in TIMEFRAME_MAP_INT:
            return TIMEFRAME_MAP_INT[tf]
        raise ValueError(f"timeframe numérico inválido: {tf}")
    tf_str = str(tf).upper()
    if tf_str in TIMEFRAME_MAP_STR:
        return TIMEFRAME_MAP_STR[tf_str]
    raise ValueError(f"timeframe inválido: {tf}")


def _validate_count(count: int) -> None:
    if count <= 0:
        raise HTTPException(status_code=400, detail="count deve ser > 0")


def _validate_range(start: datetime, end: datetime) -> None:
    if end <= start:
        raise HTTPException(
            status_code=400,
            detail="intervalo de datas inválido: end deve ser maior que start",
        )


# ------------------------------------------------------------
# Schemas (Pydantic)
# ------------------------------------------------------------
class BulkUpdateRequest(BaseModel):
    symbols: Optional[List[str]] = None


class BulkUpdateResponse(BaseModel):
    ok: bool
    data: Dict[str, Any]


class LatestPriceResponse(BaseModel):
    symbol: str
    price: float


class RatesRequest(BaseModel):
    symbol: str
    timeframe: Union[int, TimeframeLiteral] = "M5"
    count: int = 200


class RatesResponse(BaseModel):
    symbol: str
    # mantemos timeframe como int na resposta (constante MT5),
    # pra não quebrar quem já consome isso
    timeframe: int
    rates: List[Dict[str, Any]]


class RatesRangeRequest(BaseModel):
    symbol: str
    timeframe: Literal["M1", "M5", "M15", "M30", "H1", "D1"] = "M5"
    start: datetime
    end: datetime


class RatesRangeResponse(BaseModel):
    symbol: str
    timeframe: int
    rates: List[Dict[str, Any]]


TradeSideLiteral = Literal["buy", "sell"]
TradeTimeLiteral = Literal["GTC", "DAY", "SPECIFIED"]
TradeFillingLiteral = Literal["IOC", "FOK", "RETURN"]


class TradeOrder(BaseModel):
    symbol: str
    side: TradeSideLiteral
    lots: Optional[float] = None
    quantity: Optional[int] = None
    lot_size: Optional[int] = None
    price: Optional[float] = None
    deviation: Optional[int] = None
    comment: Optional[str] = None
    type_time: TradeTimeLiteral = "GTC"
    type_filling: TradeFillingLiteral = "IOC"


class TradesRequest(BaseModel):
    trades: List[TradeOrder]


class TradeResult(BaseModel):
    symbol: str
    ticket: int
    retcode: int
    price: float
    volume: float
    comment: Optional[str] = None


class TradesResponse(BaseModel):
    trades: List[TradeResult]


# ------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------
@app.get("/api/ping")
def ping():
    return {"status": "ok", "message": "mt5_bridge rodando"}


@app.get("/api/latest_price/{symbol}", response_model=LatestPriceResponse)
def latest_price(symbol: str):
    price = quotes_core.get_latest_price(symbol)
    if price is None:
        raise HTTPException(status_code=404, detail="Preço não encontrado para esse símbolo")

    return LatestPriceResponse(symbol=symbol, price=price)


@app.post("/api/bulk_update_quotes", response_model=BulkUpdateResponse)
def bulk_update_quotes(payload: BulkUpdateRequest):
    data = quotes_core.bulk_update_quotes(symbols=payload.symbols)
    return BulkUpdateResponse(ok=True, data=data)


@app.post("/api/rates", response_model=RatesResponse)
def rates(payload: RatesRequest):
    # validação de parâmetros do cliente (400)
    _validate_count(payload.count)
    try:
        timeframe_const = _resolve_timeframe(payload.timeframe)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    logger.info(
        "Chamando fetch_rates: symbol=%s timeframe=%s count=%s",
        payload.symbol,
        timeframe_const,
        payload.count,
    )

    try:
        data = quotes_core.fetch_rates(payload.symbol, timeframe_const, payload.count)
    except RuntimeError as exc:
        detail = str(exc)
        logger.exception(
            "Erro MT5 em /api/rates para symbol=%s timeframe=%s count=%s: %s",
            payload.symbol,
            timeframe_const,
            payload.count,
            detail,
        )
        raise HTTPException(status_code=422, detail=detail)
    except Exception:
        logger.exception(
            "Erro inesperado em /api/rates para symbol=%s timeframe=%s count=%s",
            payload.symbol,
            timeframe_const,
            payload.count,
        )
        raise HTTPException(status_code=500, detail="Erro interno na MT5 bridge")

    return RatesResponse(
        symbol=payload.symbol,
        timeframe=timeframe_const,
        rates=data,
    )


@app.post("/api/rates/range", response_model=RatesRangeResponse)
def rates_range(payload: RatesRangeRequest):
    # validação de parâmetros do cliente (400)
    timeframe_const = _resolve_timeframe(payload.timeframe)
    _validate_range(payload.start, payload.end)

    try:
        data = quotes_core.fetch_rates_range(
            payload.symbol,
            timeframe_const,
            payload.start,
            payload.end,
        )
    except RuntimeError as exc:
        detail = str(exc)
        logger.error(
            "rates_range /api/rates/range falhou para %s (timeframe=%s start=%s end=%s): %s",
            payload.symbol,
            timeframe_const,
            payload.start,
            payload.end,
            detail,
        )
        # erro vindo do MT5 -> 422
        raise HTTPException(status_code=422, detail=detail)

    return RatesRangeResponse(
        symbol=payload.symbol,
        timeframe=timeframe_const,
        rates=data,
    )


_TIME_MAP: Dict[str, int] = {
    "GTC": mt5.ORDER_TIME_GTC,
    "DAY": mt5.ORDER_TIME_DAY,
    "SPECIFIED": mt5.ORDER_TIME_SPECIFIED,
}

_FILLING_MAP: Dict[str, int] = {
    "IOC": mt5.ORDER_FILLING_IOC,
    "FOK": mt5.ORDER_FILLING_FOK,
    "RETURN": mt5.ORDER_FILLING_RETURN,
}


def _resolve_volume(order: TradeOrder) -> float:
    if order.lots is not None and order.lots > 0:
        return float(order.lots)
    if order.quantity and order.quantity > 0:
        if order.lot_size and order.lot_size > 0:
            volume = order.quantity / order.lot_size
        else:
            volume = float(order.quantity)
        if volume > 0:
            return float(volume)
    raise HTTPException(status_code=400, detail="Volume da ordem deve ser especificado.")


def _resolve_price(order: TradeOrder, symbol: str) -> float:
    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        raise HTTPException(status_code=400, detail=f"Tick indisponível para {symbol}")
    if order.price is not None and order.price > 0:
        return float(order.price)
    price = tick.ask if order.side == "buy" else tick.bid
    if price and price > 0:
        return float(price)
    raise HTTPException(status_code=400, detail=f"Não foi possível determinar o preço para {symbol}")


def _order_type(order: TradeOrder) -> int:
    return mt5.ORDER_TYPE_BUY if order.side == "buy" else mt5.ORDER_TYPE_SELL


def _execute_trade(order: TradeOrder) -> TradeResult:
    symbol = order.symbol.strip().upper()
    if not quotes_core._ensure_symbol(symbol):
        raise HTTPException(status_code=400, detail=f"Símbolo {symbol} indisponível no MT5.")

    volume = _resolve_volume(order)
    price = _resolve_price(order, symbol)
    trade_request: dict[str, object] = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": _order_type(order),
        "price": price,
        "deviation": order.deviation or TRADE_DEFAULT_DEVIATION,
        "magic": TRADE_MAGIC,
        "comment": order.comment or TRADE_COMMENT,
        "type_time": _TIME_MAP.get(order.type_time, mt5.ORDER_TIME_GTC),
        "type_filling": _FILLING_MAP.get(order.type_filling, mt5.ORDER_FILLING_IOC),
    }

    result = mt5.order_send(trade_request)
    if result is None:
        raise HTTPException(status_code=500, detail="MT5 não respondeu à ordem.")
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        detail = result.comment or quotes_core._format_mt5_error()
        raise HTTPException(status_code=422, detail=detail)

    return TradeResult(
        symbol=symbol,
        ticket=int(result.order),
        retcode=int(result.retcode),
        price=float(result.price),
        volume=float(result.volume),
        comment=result.comment,
    )


@app.post("/api/trades", response_model=TradesResponse)
def trades(payload: TradesRequest):
    if not payload.trades:
        raise HTTPException(status_code=400, detail="Nenhuma ordem informada.")
    results: List[TradeResult] = []
    for order in payload.trades:
        results.append(_execute_trade(order))
    return TradesResponse(trades=results)


@app.get("/api/debug/mt5_last_error")
def mt5_last_error():
    """Retorna a última mensagem de erro do MT5."""
    return {"last_error": quotes_core.get_mt5_last_error()}
