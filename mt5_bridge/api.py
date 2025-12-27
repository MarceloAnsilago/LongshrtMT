from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .mt5_session import init_mt5
from . import quotes_core


app = FastAPI(title="MT5 Bridge", version="0.1.0")


# ------------------------------------------------------------
# Eventos de ciclo de vida
# ------------------------------------------------------------
@app.on_event("startup")
def startup_event():
    # inicializa conexão com o MT5 quando o servidor sobe
    init_mt5()


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
    timeframe: int
    count: int


class RatesResponse(BaseModel):
    symbol: str
    timeframe: int
    rates: List[Dict[str, Any]]


class RatesRangeRequest(BaseModel):
    symbol: str
    timeframe: int
    start: datetime
    end: datetime


class RatesRangeResponse(BaseModel):
    symbol: str
    timeframe: int
    rates: List[Dict[str, Any]]


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
    try:
        data = quotes_core.fetch_rates(payload.symbol, payload.timeframe, payload.count)
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return RatesResponse(symbol=payload.symbol, timeframe=payload.timeframe, rates=data)


@app.post("/api/rates/range", response_model=RatesRangeResponse)
def rates_range(payload: RatesRangeRequest):
    try:
        data = quotes_core.fetch_rates_range(payload.symbol, payload.timeframe, payload.start, payload.end)
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return RatesRangeResponse(symbol=payload.symbol, timeframe=payload.timeframe, rates=data)
