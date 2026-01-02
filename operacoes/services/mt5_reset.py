from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import MetaTrader5 as mt5
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from mt5_bridge_client.mt5client import (
    MT5BridgeError,
    fetch_account_info,
    fetch_history_deals,
    fetch_positions,
)

from operacoes.models import (
    MT5AuditEvent,
    MT5IncidentEvent,
    Operation,
    OperationMT5Trade,
)

logger = logging.getLogger(__name__)

DETECTION_WINDOW_PRE = timedelta(minutes=5)
DETECTION_WINDOW_POST = timedelta(minutes=2)
RESET_CLOSE_REASON = "DEMO_RESET_NO_DEAL_OUT"

_MANUAL_REASON_CODES = {
    mt5.DEAL_REASON_CLIENT,
    mt5.DEAL_REASON_MOBILE,
    mt5.DEAL_REASON_WEB,
}

_SERVER_REASON_CODES = {
    mt5.DEAL_REASON_SL,
    mt5.DEAL_REASON_TP,
    mt5.DEAL_REASON_SO,
}


def fetch_mt5_positions() -> list[dict[str, Any]]:
    return fetch_positions()


def fetch_mt5_history_deals(from_dt: datetime, to_dt: datetime) -> list[dict[str, Any]]:
    return fetch_history_deals(from_dt, to_dt)


def fetch_mt5_account_info() -> dict[str, Any]:
    return fetch_account_info()


def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _minimum_reset_age() -> timedelta:
    seconds = getattr(settings, "MT5_RESET_MIN_AGE_SECONDS", 180)
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        seconds = 180
    return timedelta(seconds=seconds)


def find_out_deal(
    identifiers: Sequence[int | None],
    from_dt: datetime,
    to_dt: datetime,
) -> Tuple[Optional[dict[str, Any]], list[dict[str, Any]]]:
    normalized = {ident for ident in (_coerce_int(value) for value in identifiers) if ident is not None}
    if not normalized:
        return None, []
    deals = fetch_mt5_history_deals(from_dt, to_dt)
    matches = []
    for deal_item in deals:
        if _deal_matches_identifiers(deal_item, normalized):
            matches.append(deal_item)
    if not matches:
        return None, deals
    latest = max(matches, key=lambda deal: deal.get("timestamp") or from_dt)
    return latest, deals


def _deal_matches_identifiers(deal: dict[str, Any], identifiers: Iterable[int]) -> bool:
    entry = deal.get("entry")
    if entry != mt5.DEAL_ENTRY_OUT:
        return False
    for attr in ("position_id", "order", "deal", "ticket"):
        value = deal.get(attr)
        if value is None:
            continue
        value_int = _coerce_int(value)
        if value_int in identifiers:
            return True
    return False


def classify_close(deal_reason: int | None, has_audit_event: bool) -> str:
    if deal_reason in _SERVER_REASON_CODES:
        return "sl_tp_so"
    if deal_reason in _MANUAL_REASON_CODES:
        return "manual"
    if deal_reason == mt5.DEAL_REASON_EXPERT:
        return "normal_close" if has_audit_event else "sl_tp_so"
    return "normal_close"


def _build_position_sets(positions: Sequence[dict[str, Any]]) -> Tuple[set[int], set[int]]:
    tickets = set()
    position_ids = set()
    for position in positions:
        ticket = _coerce_int(position.get("ticket"))
        if ticket:
            tickets.add(ticket)
        position_id = _coerce_int(position.get("position_id"))
        if position_id:
            position_ids.add(position_id)
    return tickets, position_ids


def _position_tickets(positions: Sequence[dict[str, Any]]) -> list[int]:
    tickets: list[int] = []
    for position in positions:
        ticket_value = _coerce_int(position.get("ticket"))
        if ticket_value is not None:
            tickets.append(ticket_value)
    return tickets


def _normalize_account_snapshot(info: dict[str, Any]) -> dict[str, Any]:
    return {
        "login": _coerce_int(info.get("login")),
        "server": info.get("server") or "",
        "balance": _coerce_float(info.get("balance")),
        "equity": _coerce_float(info.get("equity")),
        "margin": _coerce_float(info.get("margin")),
        "margin_free": _coerce_float(info.get("margin_free")),
        "margin_mode": _coerce_int(info.get("margin_mode")),
    }


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _find_audit_event(order_id: int | None, deal_id: int | None) -> Optional[MT5AuditEvent]:
    query: Optional[Q] = None
    if order_id is not None:
        query = Q(order=order_id)
    if deal_id is not None:
        deal_q = Q(deal=deal_id)
        query = deal_q if query is None else query | deal_q
    if not query:
        return None
    return MT5AuditEvent.objects.filter(query).order_by("-created_at").first()


def detect_demo_reset_for_open_trades(
    *,
    request_id: str | None = None,
    now: datetime | None = None,
) -> list[MT5IncidentEvent]:
    current = now or timezone.now()
    positions = fetch_mt5_positions() or []
    tickets, position_ids = _build_position_sets(positions)
    try:
        account_snapshot = _normalize_account_snapshot(fetch_mt5_account_info())
    except MT5BridgeError as exc:
        logger.warning("Failed to capture account info for demo reset detection: %s", exc)
        account_snapshot = {}
    events: list[MT5IncidentEvent] = []
    trades = OperationMT5Trade.objects.filter(status=OperationMT5Trade.STATUS_OPEN).select_related("operation")
    for trade in trades:
        identifiers = (trade.ticket, trade.position_id)
        from_dt = trade.opened_at - DETECTION_WINDOW_PRE
        to_dt = current + DETECTION_WINDOW_POST
        in_positions = (
            bool(trade.ticket and trade.ticket in tickets)
            or bool(trade.position_id and trade.position_id in position_ids)
        )
        deal = None
        history_error = False
        found_deals: list[dict[str, Any]] = []
        if not in_positions:
            try:
                deal, found_deals = find_out_deal(identifiers, from_dt, to_dt)
            except MT5BridgeError as exc:
                logger.warning("MT5 history fetch failed during reset detection: %s", exc)
                history_error = True
        deal_reason = _coerce_int(deal.get("reason") if deal else None)
        audit_event = _find_audit_event(
            _coerce_int(deal.get("order") if deal else None),
            _coerce_int(deal.get("deal") if deal else None),
        )
        classification = "normal_close"
        if in_positions:
            classification = "normal_close"
        elif deal:
            classification = classify_close(deal_reason, bool(audit_event))
        else:
            classification = "reset_demo_suspeito"
        payload = {
            "positions": _position_tickets(positions),
            "found_out_deal": bool(deal),
            "checked_deals": len(found_deals),
            "history_error": history_error,
        }
        log_payload = {
            "request_id": request_id,
            "operation_id": trade.operation_id,
            "ticket": trade.ticket,
            "position_id": trade.position_id,
            "symbol": trade.symbol,
            "in_db_open": True,
            "in_mt5_positions": in_positions,
            "found_out_deal": bool(deal),
            "classification": classification,
            "history_error": history_error,
        }
        logger.info("MT5DemoReset %s", json.dumps(log_payload, default=str, ensure_ascii=False))
        should_mark_reset = (
            not in_positions
            and not deal
            and not history_error
            and (current - trade.opened_at) >= _minimum_reset_age()
        )
        if not should_mark_reset:
            continue

        with transaction.atomic():
            trade.status = OperationMT5Trade.STATUS_RESET
            trade.closed_at = current
            trade.close_reason = RESET_CLOSE_REASON
            trade.save(update_fields=["status", "closed_at", "close_reason"])
            event = MT5IncidentEvent.objects.create(
                operation=trade.operation,
                trade=trade,
                ticket=trade.ticket,
                position_id=trade.position_id,
                opened_at=trade.opened_at,
                account_login=str(account_snapshot.get("login") or ""),
                account_server=account_snapshot.get("server") or "",
                balance=account_snapshot.get("balance"),
                equity=account_snapshot.get("equity"),
                margin=account_snapshot.get("margin"),
                margin_free=account_snapshot.get("margin_free"),
                margin_mode=account_snapshot.get("margin_mode"),
                positions_total=len(positions),
                from_dt=from_dt,
                to_dt=to_dt,
                payload=payload,
                classification=classification,
            )
            if trade.operation and not trade.operation.mt5_trades.filter(
                status=OperationMT5Trade.STATUS_OPEN
            ).exists():
                operation = trade.operation
                operation.status = Operation.STATUS_CLOSED
                operation.save(update_fields=["status"])
            events.append(event)
    return events
