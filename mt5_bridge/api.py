from datetime import datetime, timezone, timedelta
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

def _log_account_context() -> None:
    info = mt5.account_info()
    if not info:
        logger.warning("MT5 account info unavailable at startup.")
        return
    margin_label = _MARGIN_MODE_LABELS.get(info.margin_mode, str(info.margin_mode))
    logger.info(
        "MT5 account login=%s server=%s margin_mode=%s",
        getattr(info, "login", None),
        getattr(info, "server", None),
        margin_label,
    )

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

TRADE_MAGIC = _env_int("MT5_TRADE_MAGIC", 741853)
TRADE_DEFAULT_DEVIATION = _env_int("MT5_TRADE_DEVIATION", 20)
TRADE_COMMENT = os.environ.get("MT5_TRADE_COMMENT", "LongShort")

_MARGIN_MODE_LABELS: Dict[int, str] = {
    mt5.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING: "RETAIL_HEDGING",
    mt5.ACCOUNT_MARGIN_MODE_RETAIL_NETTING: "RETAIL_NETTING",
}


# ------------------------------------------------------------
# Eventos de ciclo de vida
# ------------------------------------------------------------
@app.on_event("startup")
def startup_event():
    # inicializa conexão com o MT5 quando o servidor sobe
    init_mt5()
    _log_account_context()


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
    request_id: Optional[str] = None


class TradesRequest(BaseModel):
    trades: List[TradeOrder]


class TradeResult(BaseModel):
    symbol: str
    ticket: int
    retcode: int
    price: float
    volume: float
    comment: Optional[str] = None
    order: Optional[int] = None
    deal: Optional[int] = None
    position_id: Optional[int] = None
    request_id: Optional[str] = None
    account_login: Optional[int] = None
    account_server: Optional[str] = None


class TradesResponse(BaseModel):
    trades: List[TradeResult]


class ExplainCloseDeal(BaseModel):
    timestamp: datetime
    symbol: Optional[str] = None
    price: float
    profit: float
    volume: float
    comment: Optional[str] = None
    magic: Optional[int] = None
    order: Optional[int] = None
    deal: Optional[int] = None
    position_id: Optional[int] = None
    deal_type: Optional[int] = None
    deal_reason: Optional[int] = None
    deal_entry: Optional[int] = None
    deal_position_id: Optional[int] = None
    deal_comment: Optional[str] = None
    deal_magic: Optional[int] = None


class ExplainCloseRequest(BaseModel):
    identifier: int
    from_dt: datetime
    to_dt: datetime


class ExplainCloseResponse(BaseModel):
    identifier: int
    deal: ExplainCloseDeal


class HistoryDealsRequest(BaseModel):
    from_dt: datetime
    to_dt: datetime
    symbol: Optional[str] = None


class HistoryDeal(BaseModel):
    timestamp: Optional[datetime]
    symbol: Optional[str] = None
    price: Optional[float] = None
    volume: Optional[float] = None
    profit: Optional[float] = None
    entry: Optional[int] = None
    reason: Optional[int] = None
    magic: Optional[int] = None
    order: Optional[int] = None
    deal: Optional[int] = None
    position_id: Optional[int] = None
    ticket: Optional[int] = None
    comment: Optional[str] = None


class HistoryDealsResponse(BaseModel):
    from_dt: datetime
    to_dt: datetime
    deals: List[HistoryDeal]


class PositionSummary(BaseModel):
    ticket: Optional[int]
    position: Optional[int]
    position_id: Optional[int]
    symbol: Optional[str]
    volume: Optional[float]
    price_open: Optional[float]
    price_current: Optional[float]
    price: Optional[float]
    time: Optional[datetime]
    comment: Optional[str]
    magic: Optional[int]


class PositionsResponse(BaseModel):
    positions: List[PositionSummary]


class AccountInfoResponse(BaseModel):
    login: Optional[int]
    server: Optional[str]
    balance: Optional[float]
    equity: Optional[float]
    margin: Optional[float]
    margin_free: Optional[float]
    margin_mode: Optional[int]


def _deal_matches_identifier(deal: Any, identifier: int) -> bool:
    entry = getattr(deal, "entry", None)
    if entry != mt5.DEAL_ENTRY_OUT:
        return False
    return any(
        getattr(deal, attr, None) == identifier
        for attr in ("position_id", "order", "deal")
    )


def _deal_to_summary(deal: Any) -> ExplainCloseDeal:
    timestamp = getattr(deal, "time", 0) or 0
    moment = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    return ExplainCloseDeal(
        timestamp=moment,
        symbol=getattr(deal, "symbol", None),
        price=float(getattr(deal, "price", 0.0) or 0.0),
        profit=float(getattr(deal, "profit", 0.0) or 0.0),
        volume=float(getattr(deal, "volume", 0.0) or 0.0),
        comment=getattr(deal, "comment", None),
        magic=_cast_int(getattr(deal, "magic", None)),
        order=_cast_int(getattr(deal, "order", None)),
        deal=_cast_int(getattr(deal, "deal", None)),
        position_id=_cast_int(getattr(deal, "position_id", None)),
        deal_type=_cast_int(getattr(deal, "type", None)),
        deal_reason=_cast_int(getattr(deal, "reason", None)),
        deal_entry=_cast_int(getattr(deal, "entry", None)),
        deal_position_id=_cast_int(getattr(deal, "position_id", None)),
        deal_comment=getattr(deal, "comment", None),
        deal_magic=_cast_int(getattr(deal, "magic", None)),
    )


def _cast_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _cast_timestamp(value: Any) -> datetime | None:
    try:
        if value is None:
            return None
        raw = float(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(raw, tz=timezone.utc)


def _history_deal_to_dict(deal: Any) -> HistoryDeal:
    return HistoryDeal(
        timestamp=_cast_timestamp(getattr(deal, "time", None)),
        symbol=getattr(deal, "symbol", None),
        price=_cast_float(getattr(deal, "price", None)),
        volume=_cast_float(getattr(deal, "volume", None)),
        profit=_cast_float(getattr(deal, "profit", None)),
        entry=_cast_int(getattr(deal, "entry", None)),
        reason=_cast_int(getattr(deal, "reason", None)),
        magic=_cast_int(getattr(deal, "magic", None)),
        order=_cast_int(getattr(deal, "order", None)),
        deal=_cast_int(getattr(deal, "deal", None)),
        position_id=_cast_int(getattr(deal, "position_id", None)),
        ticket=_cast_int(getattr(deal, "ticket", None)),
        comment=getattr(deal, "comment", None),
    )


def _position_to_summary(position: Any) -> PositionSummary:
    return PositionSummary(
        ticket=_cast_int(getattr(position, "ticket", None)),
        position=_cast_int(getattr(position, "position", None)),
        position_id=_cast_int(getattr(position, "position_id", None)),
        symbol=getattr(position, "symbol", None),
        volume=_cast_float(getattr(position, "volume", None)),
        price_open=_cast_float(getattr(position, "price_open", None)),
        price_current=_cast_float(getattr(position, "price_current", None)),
        price=_cast_float(getattr(position, "price", None)),
        time=_cast_timestamp(getattr(position, "time", None)),
        comment=getattr(position, "comment", None),
        magic=_cast_int(getattr(position, "magic", None)),
    )


def _select_closing_deal(
    identifier: int, start: datetime, end: datetime
) -> ExplainCloseDeal:
    try:
        deals = mt5.history_deals_get(start, end)
    except Exception as exc:
        logger.error("MT5 history deals failed: %s", exc)
        raise HTTPException(status_code=422, detail=str(exc))

    if not deals:
        raise HTTPException(
            status_code=404,
            detail="No deals returned for the requested interval.",
        )

    matches = [deal for deal in deals if _deal_matches_identifier(deal, identifier)]
    if not matches:
        raise HTTPException(
            status_code=404,
            detail="No closing deal found for the provided identifier.",
        )

    closing = max(matches, key=lambda deal: getattr(deal, "time", 0) or 0)
    return _deal_to_summary(closing)


@app.post("/api/history/explain_close", response_model=ExplainCloseResponse)
def explain_close(payload: ExplainCloseRequest):
    _validate_range(payload.from_dt, payload.to_dt)
    summary = _select_closing_deal(payload.identifier, payload.from_dt, payload.to_dt)
    return ExplainCloseResponse(identifier=payload.identifier, deal=summary)


@app.post("/api/history/deals", response_model=HistoryDealsResponse)
def history_deals(payload: HistoryDealsRequest):
    _validate_range(payload.from_dt, payload.to_dt)
    try:
        deals = mt5.history_deals_get(payload.from_dt, payload.to_dt)
    except Exception as exc:
        logger.error("MT5 history deals failed: %s", exc)
        raise HTTPException(status_code=422, detail=str(exc))
    converted = [_history_deal_to_dict(deal) for deal in deals or []]
    return HistoryDealsResponse(from_dt=payload.from_dt, to_dt=payload.to_dt, deals=converted)


@app.get("/api/positions", response_model=PositionsResponse)
def positions():
    try:
        raw = mt5.positions_get()
    except Exception as exc:
        logger.error("MT5 positions_get failed: %s", exc)
        raise HTTPException(status_code=422, detail=str(exc))
    if raw is None:
        raise HTTPException(status_code=422, detail="Positions unavailable")
    results = [_position_to_summary(position) for position in raw]
    return PositionsResponse(positions=results)


@app.get("/api/account_info", response_model=AccountInfoResponse)
def account_info():
    info = mt5.account_info()
    if not info:
        raise HTTPException(status_code=422, detail="Account info unavailable")
    return AccountInfoResponse(
        login=_cast_int(getattr(info, "login", None)),
        server=getattr(info, "server", None),
        balance=_cast_float(getattr(info, "balance", None)),
        equity=_cast_float(getattr(info, "equity", None)),
        margin=_cast_float(getattr(info, "margin", None)),
        margin_free=_cast_float(getattr(info, "margin_free", None)),
        margin_mode=_cast_int(getattr(info, "margin_mode", None)),
    )


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


def _cast_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _account_context() -> Dict[str, Any]:
    info = mt5.account_info()
    if not info:
        return {"login": None, "server": None}
    return {
        "login": getattr(info, "login", None),
        "server": getattr(info, "server", None),
    }


_DEAL_LOOKBACK = timedelta(minutes=2)


def _resolve_position_id(result: Any) -> int | None:
    position = _cast_int(getattr(result, "position", None))
    if position:
        return position
    deal_id = _cast_int(getattr(result, "deal", None))
    if not deal_id:
        return None
    end = datetime.now(timezone.utc)
    start = end - _DEAL_LOOKBACK
    try:
        deals = mt5.history_deals_get(start, end)
    except Exception as exc:
        logger.warning("Unable to fetch history deals for position_id fallback: %s", exc)
        return None
    if not deals:
        return None
    for deal in deals:
        if _cast_int(getattr(deal, "deal", None)) == deal_id:
            return _cast_int(getattr(deal, "position_id", None))
    return None


def _execute_trade(order: TradeOrder) -> TradeResult:
    symbol = order.symbol.strip().upper()
    if not quotes_core._ensure_symbol(symbol):
        raise HTTPException(status_code=400, detail=f"Symbol {symbol} unavailable in MT5.")

    volume = _resolve_volume(order)
    price = _resolve_price(order, symbol)
    account_info = _account_context()
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
    if order.request_id:
        trade_request["request_id"] = order.request_id

    result = mt5.order_send(trade_request)
    if result is None:
        raise HTTPException(status_code=500, detail="MT5 did not answer the order.")
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        detail = result.comment or quotes_core._format_mt5_error()
        raise HTTPException(status_code=422, detail=detail)

    return TradeResult(
        symbol=symbol,
        ticket=_cast_int(result.order) or 0,
        retcode=int(result.retcode),
        price=float(result.price),
        volume=float(result.volume),
        comment=result.comment,
        order=_cast_int(result.order),
        deal=_cast_int(result.deal),
        position_id=_resolve_position_id(result),
        request_id=order.request_id,
        account_login=account_info.get("login"),
        account_server=account_info.get("server"),
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
