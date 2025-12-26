from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Iterable, Optional, Callable

import MetaTrader5 as mt5
from django.db.models import Max
from django.utils import timezone

from acoes.models import Asset
from cotacoes.models import QuoteDaily, MissingQuoteLog, QuoteLive
from longshort.services.mt5_session import ensure_mt5_initialized

logger = logging.getLogger(__name__)

ProgressCB = Optional[Callable[[str, int, int, str, int], None]]

DAILY_HISTORY_COUNT = 200
DAILY_REFRESH_COUNT = 5
INTRADAY_TIMEFRAME = mt5.TIMEFRAME_M5
INTRADAY_BARS = 1
BULK_BATCH_SIZE = 1000


def _normalize_symbol(value: str | None) -> Optional[str]:
    if not value:
        return None
    normalized = value.strip().upper()
    if normalized.endswith(".SA"):
        normalized = normalized[:-3]
    return normalized or None


def _mt5_symbol_for_asset(asset) -> Optional[str]:
    symbol = _normalize_symbol(getattr(asset, "ticker", None))
    if symbol:
        return symbol
    return _normalize_symbol(getattr(asset, "ticker_yf", None))


def _format_mt5_error() -> str:
    err = mt5.last_error()
    if not err or err[0] == 0:
        return "unknown"
    return f"{err[1]} ({err[0]})"


def _log_missing_quote(asset, reason: str, detail: str, *, date=None) -> None:
    try:
        MissingQuoteLog.objects.create(
            asset=asset,
            date=date,
            reason=reason,
            detail=str(detail),
        )
    except Exception:
        logger.exception("Failed to log MissingQuoteLog for %s", asset)


def _rate_date(rate) -> Optional[datetime.date]:
    try:
        return datetime.utcfromtimestamp(rate["time"]).date()
    except Exception:
        return None


def _safe_close(rate) -> Optional[float]:
    try:
        return float(rate["close"])
    except Exception:
        return None


def _fetch_mt5_rates(symbol: str, timeframe: int, count: int) -> list[dict]:
    now = datetime.now()
    raw = mt5.copy_rates_from(symbol, timeframe, now, count)
    if raw is None:
        return []
    rates: list[dict] = []
    for row in raw:
        if hasattr(row, "_asdict"):
            rates.append(dict(row._asdict()))
            continue
        try:
            dtype = getattr(row, "dtype", None)
            if dtype and hasattr(dtype, "names"):
                rates.append({name: row[i] for i, name in enumerate(dtype.names)})
                continue
        except Exception:
            pass
        rates.append({str(i): value for i, value in enumerate(row)})
    return sorted(rates, key=lambda rate: rate.get("time", 0))


def _fetch_intraday_price(symbol: str, timeframe: int = INTRADAY_TIMEFRAME) -> Optional[float]:
    rates = _fetch_mt5_rates(symbol, timeframe, INTRADAY_BARS)
    if not rates:
        return None
    return _safe_close(rates[-1])


def _upsert_intraday_quote(asset, quote_date, price: float) -> bool:
    defaults = {"close": price, "is_provisional": True}
    obj, created = QuoteDaily.objects.update_or_create(
        asset=asset,
        date=quote_date,
        defaults=defaults,
    )
    if not created and obj.is_provisional is False:
        obj.close = price
        obj.is_provisional = True
        obj.save(update_fields=["close", "is_provisional"])
    return created


def bulk_update_quotes(
    assets: Iterable,
    period: str = "2y",
    interval: str = "1d",
    progress_cb: ProgressCB = None,
    use_stooq: bool = False,
) -> tuple[int, int]:
    assets = list(assets)
    total_assets = len(assets)
    if progress_cb:
        progress_cb("start", 0, total_assets, "starting", 0)
    if total_assets == 0:
        return 0, 0

    if not ensure_mt5_initialized():
        detail = _format_mt5_error()
        for asset in assets:
            _log_missing_quote(asset, "mt5_init_failed", detail)
        return 0, 0

    inserted_assets = 0
    total_rows = 0
    bulk_objs: list[QuoteDaily] = []

    def _flush_bulk() -> None:
        nonlocal bulk_objs
        if not bulk_objs:
            return
        QuoteDaily.objects.bulk_create(
            bulk_objs,
            ignore_conflicts=True,
            batch_size=BULK_BATCH_SIZE,
        )
        bulk_objs = []

    try:
        for idx, asset in enumerate(assets, start=1):
            ticker_label = getattr(asset, "ticker", "")
            if progress_cb:
                progress_cb(ticker_label, idx, total_assets, "processing", 0)
            logger.info("Updating quotes for %s (%s/%s)", ticker_label, idx, total_assets)

            symbol = _mt5_symbol_for_asset(asset)
            if not symbol:
                _log_missing_quote(asset, "invalid_symbol", "Ticker empty or invalid")
                logger.warning("Skipping %s: invalid symbol for MT5", ticker_label)
                if progress_cb:
                    progress_cb(ticker_label, idx, total_assets, "no_symbol", 0)
                continue

            if not mt5.symbol_select(symbol, True):
                _log_missing_quote(asset, "symbol_not_available", f"MT5 did not load {symbol}")
                logger.warning("Symbol select failed for %s (%s)", symbol, ticker_label)
                if progress_cb:
                    progress_cb(symbol, idx, total_assets, "symbol_missing", 0)
                continue

            last_date = QuoteDaily.objects.filter(asset=asset).aggregate(Max("date"))["date__max"]
            try:
                needed = DAILY_HISTORY_COUNT if last_date is None else DAILY_REFRESH_COUNT
                rates = _fetch_mt5_rates(symbol, mt5.TIMEFRAME_D1, needed)
                if not rates:
                    raise RuntimeError("No D1 bars returned")

                rows_inserted = 0
                for rate in rates:
                    bar_date = _rate_date(rate)
                    close = _safe_close(rate)
                    if bar_date is None or close is None:
                        continue
                    updated = QuoteDaily.objects.filter(asset=asset, date=bar_date).update(
                        close=close,
                        is_provisional=False,
                    )
                    if updated:
                        continue
                    bulk_objs.append(QuoteDaily(asset=asset, date=bar_date, close=close))
                    rows_inserted += 1
                    if len(bulk_objs) >= BULK_BATCH_SIZE:
                        _flush_bulk()

                today = timezone.localdate()
                if not QuoteDaily.objects.filter(asset=asset, date=today).exists():
                    intraday = _fetch_intraday_price(symbol)
                    if intraday is not None:
                        created = _upsert_intraday_quote(asset, today, intraday)
                        if created:
                            rows_inserted += 1

                if rows_inserted > 0:
                    total_rows += rows_inserted
                    inserted_assets += 1
                    logger.info("Inserted %s quotes (%s rows)", symbol, rows_inserted)
                    if progress_cb:
                        progress_cb(symbol, idx, total_assets, "ok", rows_inserted)
                else:
                    logger.info("No new rows for %s (up to date)", symbol)
                    if progress_cb:
                        progress_cb(symbol, idx, total_assets, "up_to_date", 0)

            except Exception as exc:  # pragma: no cover - asset errors do not stop the pipeline
                logger.exception("Error updating quotes for %s", symbol)
                _log_missing_quote(asset, "mt5_error", str(exc))
                if progress_cb:
                    progress_cb(symbol, idx, total_assets, "error", 0)
    finally:
        _flush_bulk()

    if progress_cb:
        progress_cb("done", total_assets, total_assets, "done", total_rows)
    return inserted_assets, total_rows


def update_live_quotes(assets: Iterable, progress_cb: ProgressCB = None) -> tuple[int, int]:
    assets = list(assets)
    total = len(assets)
    updated = 0

    if progress_cb:
        progress_cb("start", 0, total, "starting_live", 0)

    if not ensure_mt5_initialized():
        logger.error("MT5 initialize failed: %s", _format_mt5_error())
        return 0, total

    try:
        for idx, asset in enumerate(assets, start=1):
            ticker_label = getattr(asset, "ticker", "")
            if progress_cb:
                progress_cb(ticker_label, idx, total, "processing_live", updated)

            symbol = _mt5_symbol_for_asset(asset)
            if not symbol or not mt5.symbol_select(symbol, True):
                if progress_cb:
                    progress_cb(symbol or ticker_label, idx, total, "symbol_missing", 0)
                continue

            price = _fetch_intraday_price(symbol)
            if price is None:
                if progress_cb:
                    progress_cb(symbol, idx, total, "no_data", 0)
                continue

            QuoteLive.objects.update_or_create(asset=asset, defaults={"price": price})
            updated += 1
            if progress_cb:
                progress_cb(symbol, idx, total, "ok", updated)
    finally:
        mt5.shutdown()

    if progress_cb:
        progress_cb("done", total, total, "done_live", updated)
    return updated, total


def fetch_latest_price(ticker: str) -> Optional[float]:
    symbol = _normalize_symbol(ticker)
    if not symbol:
        return None

    if not ensure_mt5_initialized():
        logger.error("MT5 initialize failed: %s", _format_mt5_error())
        return None

    if not mt5.symbol_select(symbol, True):
        return None
    return _fetch_intraday_price(symbol)


def _fetch_mt5_rates_range(symbol: str, timeframe: int, start_dt: datetime, end_dt: datetime) -> list[dict]:
    raw = mt5.copy_rates_range(symbol, timeframe, start_dt, end_dt)
    if raw is None:
        return []
    rates = [dict(row._asdict()) if hasattr(row, "_asdict") else dict(row) for row in raw]
    return sorted(rates, key=lambda rate: rate.get("time", 0))


def _business_days(start: date, end: date) -> list[date]:
    if start > end:
        return []
    cursor = start
    days: list[date] = []
    while cursor <= end:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _date_to_unix(value: date) -> int:
    return int(datetime(value.year, value.month, value.day).timestamp())


def _try_fetch_single_date_internal(asset, symbol: str, quote_date: date) -> bool:
    if not mt5.symbol_select(symbol, True):
        return False
    start = datetime(quote_date.year, quote_date.month, quote_date.day)
    end = start + timedelta(days=1)
    rates = _fetch_mt5_rates_range(symbol, mt5.TIMEFRAME_D1, start, end)
    if not rates:
        return False
    close_value = _safe_close(rates[-1])
    if close_value is None:
        return False
    QuoteDaily.objects.update_or_create(
        asset=asset,
        date=quote_date,
        defaults={"close": close_value, "is_provisional": False},
    )
    return True


def try_fetch_single_date(asset, quote_date: date, *, use_stooq: bool = False) -> bool:
    symbol = _mt5_symbol_for_asset(asset)
    if not symbol:
        return False
    if not ensure_mt5_initialized():
        logger.error("MT5 initialize failed: %s", _format_mt5_error())
        return False
    try:
        return _try_fetch_single_date_internal(asset, symbol, quote_date)
    finally:
        mt5.shutdown()


def find_missing_dates_for_asset(asset, *, since_months: int | None = 18) -> list[date]:
    today = timezone.localdate()
    if since_months:
        lookback_start = today - timedelta(days=since_months * 30)
    else:
        lookback_start = today - timedelta(days=365)
    results = QuoteDaily.objects.filter(asset=asset, date__range=(lookback_start, today)).values_list("date", flat=True)
    existing = set(results)
    return [day for day in _business_days(lookback_start, today) if day not in existing]


def scan_all_assets_and_fix(*, use_stooq: bool = False, since_months: int | None = 18) -> list[dict]:
    assets = Asset.objects.filter(is_active=True).order_by("ticker")
    initialized = ensure_mt5_initialized()
    detail = _format_mt5_error() if not initialized else ""
    results: list[dict] = []
    for asset in assets:
        missing = find_missing_dates_for_asset(asset, since_months=since_months)
        missing_before = len(missing)
        fixed = 0
        remaining: list[str] = []
        symbol = _mt5_symbol_for_asset(asset)
        if not symbol:
            remaining = [day.isoformat() for day in missing]
        elif missing:
            if not initialized:
                remaining = [day.isoformat() for day in missing]
                _log_missing_quote(asset, "mt5_init_failed", detail or "MT5 not available")
            else:
                for day in missing:
                    if _try_fetch_single_date_internal(asset, symbol, day):
                        fixed += 1
                    else:
                        remaining.append(day.isoformat())
        results.append(
            {
                "ticker": asset.ticker,
                "missing_before": missing_before,
                "fixed": fixed,
                "remaining": remaining,
            }
        )
    return results
