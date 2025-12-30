from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

import MetaTrader5 as mt5
from django.conf import settings
from django.db.models import Q

from mt5_bridge_client.mt5client import MT5BridgeError, explain_close as bridge_explain_close
from operacoes.models import MT5AuditEvent, OperationMT5Trade

logger = logging.getLogger(__name__)

__all__ = ["explain_close", "who_closed"]


_REASON_LABELS: Dict[int, str] = {
    mt5.DEAL_REASON_CLIENT: "CLIENT",
    mt5.DEAL_REASON_MOBILE: "MOBILE",
    mt5.DEAL_REASON_WEB: "WEB",
    mt5.DEAL_REASON_EXPERT: "EXPERT",
    mt5.DEAL_REASON_SL: "SL",
    mt5.DEAL_REASON_TP: "TP",
    mt5.DEAL_REASON_SO: "SO",
}

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


def _cast_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _entry_label(entry: int | None) -> Optional[str]:
    if entry == mt5.DEAL_ENTRY_IN:
        return "IN"
    if entry == mt5.DEAL_ENTRY_OUT:
        return "OUT"
    return None


def _reason_label(reason: int | None) -> str:
    if reason is None:
        return "UNKNOWN"
    return _REASON_LABELS.get(reason, f"REASON_{reason}")


def _find_trade(ticket: int | None) -> OperationMT5Trade | None:
    if ticket is None:
        return None
    return OperationMT5Trade.objects.filter(ticket=ticket).first()


def _find_audit_event(order_id: int | None, deal_id: int | None) -> MT5AuditEvent | None:
    query: Optional[Q] = None
    if order_id is not None:
        query = Q(order=order_id)
    if deal_id is not None:
        q = Q(deal=deal_id)
        query = q if query is None else query | q
    if not query:
        return None
    return (
        MT5AuditEvent.objects.filter(query)
        .order_by("-created_at")
        .first()
    )


def _classify_origin(reason_code: int | None, audit_event: MT5AuditEvent | None) -> str:
    if audit_event:
        return "app"
    if reason_code in _MANUAL_REASON_CODES:
        return "manual"
    if reason_code in _SERVER_REASON_CODES:
        return "sl/tp/so"
    if reason_code == mt5.DEAL_REASON_EXPERT:
        return "server_expert"
    return "unknown"


def _fetch_close_response(identifier: int, from_dt: datetime, to_dt: datetime) -> Dict[str, Any]:
    try:
        return bridge_explain_close(identifier, from_dt, to_dt)
    except MT5BridgeError as exc:
        logger.warning("MT5 explain_close failed for %s: %s", identifier, exc)
        raise


def _infer_heuristic(deal: Dict[str, Any], trade: OperationMT5Trade | None) -> str:
    price = deal.get("price")
    sl_tp_reason = ""
    if trade and price is not None:
        if _is_near(float(price), trade.sl) or _is_near(float(price), trade.tp):
            sl_tp_reason = "SL/TP"

    comment = str(deal.get("deal_comment") or deal.get("comment") or "")
    magic = deal.get("deal_magic") or deal.get("magic")
    bot_reason = False
    magic_value = getattr(settings, "MT5_TRADE_MAGIC", None)
    trade_comment_prefix = getattr(settings, "MT5_TRADE_COMMENT", "LongShort")
    if magic is not None and magic_value is not None and int(magic) == int(magic_value):
        bot_reason = True
    if trade_comment_prefix and trade_comment_prefix in comment:
        bot_reason = True
    if sl_tp_reason:
        return sl_tp_reason
    if bot_reason:
        return "bot/system"
    return "manual/unknown"


def explain_close(ticket_or_position_id: int, from_dt: datetime, to_dt: datetime) -> Dict[str, Any]:
    response = _fetch_close_response(ticket_or_position_id, from_dt, to_dt)
    deal = response.get("deal") or {}
    trade = _find_trade(_cast_int(deal.get("order")) or _cast_int(deal.get("deal")))
    result = {
        "identifier": response.get("identifier", ticket_or_position_id),
        "timestamp": deal.get("timestamp"),
        "symbol": deal.get("symbol"),
        "price": deal.get("price"),
        "profit": deal.get("profit"),
        "volume": deal.get("volume"),
        "comment": deal.get("deal_comment") or deal.get("comment"),
        "magic": _cast_int(deal.get("deal_magic") or deal.get("magic")),
        "order": _cast_int(deal.get("order")),
        "deal": _cast_int(deal.get("deal")),
        "position_id": _cast_int(deal.get("deal_position_id") or deal.get("position_id")),
        "deal_reason": _reason_label(_cast_int(deal.get("deal_reason"))),
        "deal_entry": _entry_label(_cast_int(deal.get("deal_entry"))),
        "heuristic": _infer_heuristic(deal, trade),
        "operation_id": trade.operation_id if trade else None,
        "leg": trade.leg if trade else None,
    }
    return result


def who_closed(position_id: int, from_dt: datetime, to_dt: datetime) -> Dict[str, Any]:
    response = _fetch_close_response(position_id, from_dt, to_dt)
    deal = response.get("deal") or {}
    order_id = _cast_int(deal.get("order"))
    deal_id = _cast_int(deal.get("deal"))
    reason_code = _cast_int(deal.get("deal_reason"))
    entry_code = _cast_int(deal.get("deal_entry"))
    audit_event = _find_audit_event(order_id, deal_id)
    trade = _find_trade(order_id or deal_id)
    origin = _classify_origin(reason_code, audit_event)
    return {
        "identifier": response.get("identifier", position_id),
        "symbol": deal.get("symbol"),
        "open_at": trade.opened_at.isoformat() if trade else None,
        "close_at": deal.get("timestamp"),
        "reason": _reason_label(reason_code),
        "entry": _entry_label(entry_code),
        "comment": deal.get("deal_comment") or deal.get("comment"),
        "magic": _cast_int(deal.get("deal_magic") or deal.get("magic")),
        "order": order_id,
        "deal": deal_id,
        "position_id": _cast_int(deal.get("deal_position_id") or deal.get("position_id")),
        "audit_event_id": audit_event.id if audit_event else None,
        "origin": origin,
        "operation_id": trade.operation_id if trade else None,
        "leg": trade.leg if trade else None,
    }


def _is_near(value: float | None, target: float | None) -> bool:
    if value is None or target is None:
        return False
    tolerance = max(abs(target) * 0.0002, 0.01)
    return abs(value - target) <= tolerance
