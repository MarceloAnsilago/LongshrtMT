from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Iterable, Optional, Callable, Tuple, List

from django.db.models import Max
from django.utils import timezone

from acoes.models import Asset
from cotacoes.models import QuoteDaily, MissingQuoteLog, QuoteLive
from mt5api.mt5client import (
    MT5BridgeError,
    fetch_rates,
    fetch_rates_range,
    get_latest_price as bridge_latest_price,
)

logger = logging.getLogger(__name__)

ProgressCB = Optional[Callable[[str, int, int, str, int], None]]

DAILY_HISTORY_COUNT = 200
DAILY_REFRESH_COUNT = 5
INTRADAY_TIMEFRAME = 5  # M5
INTRADAY_BARS = 1
DAILY_TIMEFRAME = 1440  # D1
BULK_BATCH_SIZE = 1000
MAX_QUOTES_PER_ASSET = 220


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
        return float(rate.get("close"))
    except Exception:
        return None


def _fetch_bridge_rates(symbol: str, timeframe: int, count: int) -> Tuple[List[dict], Optional[str]]:
    try:
        return fetch_rates(symbol, timeframe, count), None
    except MT5BridgeError as exc:
        detail = str(exc)
        logger.warning("MT5 bridge failed to fetch rates for %s: %s", symbol, detail)
        return [], detail


def _fetch_bridge_rates_range(
    symbol: str, timeframe: int, start: datetime, end: datetime
) -> Tuple[List[dict], Optional[str]]:
    try:
        return fetch_rates_range(symbol, timeframe, start, end), None
    except MT5BridgeError as exc:
        detail = str(exc)
        logger.warning("MT5 bridge failed to fetch range for %s: %s", symbol, detail)
        return [], detail


def _fetch_intraday_price(symbol: str, timeframe: int = INTRADAY_TIMEFRAME) -> Optional[float]:
    rates, _ = _fetch_bridge_rates(symbol, timeframe, INTRADAY_BARS)
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


def _prune_old_quotes(asset, *, max_rows: int = MAX_QUOTES_PER_ASSET) -> int:
    ids = list(
        QuoteDaily.objects.filter(asset=asset)
        .order_by('-date')
        .values_list('id', flat=True)[max_rows:]
    )
    if not ids:
        return 0
    deleted, _ = QuoteDaily.objects.filter(id__in=ids).delete()
    return deleted


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

            last_date = QuoteDaily.objects.filter(asset=asset).aggregate(Max("date"))["date__max"]
            try:
                needed = DAILY_HISTORY_COUNT if last_date is None else DAILY_REFRESH_COUNT
                rates, detail = _fetch_bridge_rates(symbol, DAILY_TIMEFRAME, needed)
                if not rates:
                    _log_missing_quote(asset, "mt5_error", detail or "No D1 bars returned")
                    if progress_cb:
                        progress_cb(symbol, idx, total_assets, "error", 0)
                    continue

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
                # Avoid creating daily quotes on weekends when there is no trading session.
                if today.weekday() < 5 and not QuoteDaily.objects.filter(asset=asset, date=today).exists():
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
                try:
                    deleted = _prune_old_quotes(asset)
                    if deleted:
                        logger.info("Pruned %s old quotes for %s", deleted, symbol)
                except Exception:
                    logger.exception("Failed pruning quotes for %s", symbol)
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

    for idx, asset in enumerate(assets, start=1):
        ticker_label = getattr(asset, "ticker", "")
        if progress_cb:
            progress_cb(ticker_label, idx, total, "processing_live", updated)

        symbol = _mt5_symbol_for_asset(asset)
        if not symbol:
            if progress_cb:
                progress_cb(ticker_label, idx, total, "symbol_missing", 0)
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

    if progress_cb:
        progress_cb("done", total, total, "done_live", updated)
    return updated, total


def fetch_latest_price(ticker: str) -> Optional[float]:
    symbol = _normalize_symbol(ticker)
    if not symbol:
        return None

    try:
        return bridge_latest_price(symbol)
    except MT5BridgeError as exc:
        logger.error("MT5 bridge latest price failed for %s: %s", symbol, exc)
        return None


def _try_fetch_single_date_internal(asset, symbol: str, quote_date: date) -> bool:
    start = datetime(quote_date.year, quote_date.month, quote_date.day)
    end = start + timedelta(days=1)
    rates, detail = _fetch_bridge_rates_range(symbol, DAILY_TIMEFRAME, start, end)
    if not rates:
        logger.warning("MT5 bridge range fetch returned empty for %s: %s", symbol, detail)
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
    try:
        return _try_fetch_single_date_internal(asset, symbol, quote_date)
    except MT5BridgeError:
        return False


def find_missing_dates_for_asset(asset, *, since_months: int | None = 18) -> list[date]:
    today = timezone.localdate()
    if since_months:
        lookback_start = today - timedelta(days=since_months * 30)
    else:
        lookback_start = today - timedelta(days=365)
    results = QuoteDaily.objects.filter(asset=asset, date__range=(lookback_start, today)).values_list("date", flat=True)
    existing = set(results)
    return [day for day in _business_days(lookback_start, today) if day not in existing]


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


def scan_all_assets_and_fix(
    *,
    use_stooq: bool = False,
    since_months: int | None = 18,
    progress_cb: ProgressCB = None,
) -> list[dict]:
    assets = Asset.objects.filter(is_active=True).order_by("ticker")
    results: list[dict] = []
    total = assets.count()
    if progress_cb:
        progress_cb("start", 0, total, "starting", 0)
    for asset in assets:
        idx = len(results) + 1
        if progress_cb:
            progress_cb(asset.ticker, idx, total, "processing", 0)
        missing = find_missing_dates_for_asset(asset, since_months=since_months)
        missing_before = len(missing)
        fixed = 0
        remaining: list[str] = []
        symbol = _mt5_symbol_for_asset(asset)
        if not symbol:
            remaining = [day.isoformat() for day in missing]
            if progress_cb:
                progress_cb(asset.ticker, idx, total, "no_symbol", 0)
        elif missing:
            for day in missing:
                if _try_fetch_single_date_internal(asset, symbol, day):
                    fixed += 1
                else:
                    remaining.append(day.isoformat())
            if progress_cb:
                progress_cb(asset.ticker, idx, total, "ok", fixed)
        else:
            if progress_cb:
                progress_cb(asset.ticker, idx, total, "up_to_date", 0)
        results.append(
            {
                "ticker": asset.ticker,
                "missing_before": missing_before,
                "fixed": fixed,
                "remaining": remaining,
            }
        )
    if progress_cb:
        progress_cb("done", total, total, "done", sum(r["fixed"] for r in results))
    return results
