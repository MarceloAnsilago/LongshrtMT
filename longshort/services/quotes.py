from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Iterable, Optional, Callable

from django.db.models import Max
from django.utils import timezone

from acoes.models import Asset
from cotacoes.models import QuoteDaily, MissingQuoteLog, QuoteLive

logger = logging.getLogger(__name__)

ProgressCB = Optional[Callable[[str, int, int, str, int], None]]

MAX_QUOTES_PER_ASSET = 210


def _normalize_symbol(value: str | None) -> Optional[str]:
    if not value:
        return None
    normalized = value.strip().upper()
    if normalized.endswith(".SA"):
        normalized = normalized[:-3]
    return normalized or None


def _symbol_for_asset(asset) -> Optional[str]:
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

    synced_assets = 0
    total_missing = 0

    for idx, asset in enumerate(assets, start=1):
        ticker_label = getattr(asset, "ticker", "")
        if progress_cb:
            progress_cb(ticker_label, idx, total_assets, "processing", 0)
        logger.info("Syncing quotes for %s (%s/%s)", ticker_label, idx, total_assets)

        symbol = _symbol_for_asset(asset)
        if not symbol:
            _log_missing_quote(asset, "invalid_symbol", "Ticker empty or invalid")
            if progress_cb:
                progress_cb(ticker_label, idx, total_assets, "no_symbol", 0)
            continue

        last_date = QuoteDaily.objects.filter(asset=asset).aggregate(Max("date"))["date__max"]
        if not last_date:
            _log_missing_quote(asset, "supabase_missing", "No D1 quotes found")
            if progress_cb:
                progress_cb(symbol, idx, total_assets, "missing", 0)
            continue

        missing_dates = find_missing_dates_for_asset(asset, since_months=18)
        if missing_dates:
            total_missing += len(missing_dates)
            _log_missing_quote(
                asset,
                "missing_dates",
                f"{len(missing_dates)} dias faltando",
            )

        try:
            deleted = _prune_old_quotes(asset)
            if deleted:
                logger.info("Pruned %s old quotes for %s", deleted, symbol)
        except Exception:
            logger.exception("Failed pruning quotes for %s", symbol)

        synced_assets += 1
        if progress_cb:
            progress_cb(symbol, idx, total_assets, "ok", 0)

    if progress_cb:
        progress_cb("done", total_assets, total_assets, "done", total_missing)
    return synced_assets, 0


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

        live = QuoteLive.objects.filter(asset=asset).exists()
        if live:
            updated += 1
            if progress_cb:
                progress_cb(ticker_label, idx, total, "ok", updated)
        else:
            if progress_cb:
                progress_cb(ticker_label, idx, total, "no_data", updated)

    if progress_cb:
        progress_cb("done", total, total, "done_live", updated)
    return updated, total


def fetch_latest_price(ticker: str) -> Optional[float]:
    symbol = _normalize_symbol(ticker)
    if not symbol:
        return None
    asset = Asset.objects.filter(ticker=symbol).first()
    if not asset:
        asset = Asset.objects.filter(ticker_yf=f"{symbol}.SA").first()
    if not asset:
        return None
    live = QuoteLive.objects.filter(asset=asset).first()
    if live and live.price is not None:
        return float(live.price)
    latest = QuoteDaily.objects.filter(asset=asset).order_by("-date").first()
    if latest and latest.close is not None:
        return float(latest.close)
    return None


def try_fetch_single_date(asset, quote_date: date, *, use_stooq: bool = False) -> bool:
    return QuoteDaily.objects.filter(asset=asset, date=quote_date).exists()


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
        remaining = [day.isoformat() for day in missing]
        if progress_cb:
            status = "up_to_date" if not missing else "missing"
            progress_cb(asset.ticker, idx, total, status, fixed)
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
