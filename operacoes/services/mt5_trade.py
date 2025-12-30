from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime

from django.conf import settings
from django.utils import timezone

from acoes.models import Asset
from mt5_bridge_client.mt5client import MT5BridgeError, execute_trades
from operacoes.models import MT5AuditEvent, Operation, OperationMT5Trade
from operacoes.services.mt5_audit import (
    create_mt5_audit_event,
    update_mt5_audit_event,
)

logger = logging.getLogger(__name__)

MT5_OPEN_REASON = "strategy_entry"


class MT5TradeExecutionError(MT5BridgeError):
    """Error while executing an MT5 operation."""


def _normalize_symbol(asset: Asset | None) -> str | None:
    if asset is None:
        return None
    ticker = (asset.ticker or asset.ticker_yf or "").strip().upper()
    if not ticker:
        return None
    if ticker.endswith(".SA"):
        ticker = ticker[:-3]
    return ticker


def _build_comment(operation: Operation, role: str) -> str:
    base = getattr(settings, "MT5_TRADE_COMMENT", "LongShort")
    comment = f"{base} op#{operation.pk} {role}"
    user_id = getattr(operation.user, "id", None)
    if user_id:
        comment = f"{comment} u{user_id}"
    return comment[:31]


def _safe_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _persist_mt5_trade(operation: Operation, leg: str, payload: dict[str, object], response: dict[str, object]) -> None:
    symbol = payload.get("symbol", "")
    volume = _safe_float(response.get("volume")) or _safe_float(payload.get("lots")) or 0.0
    price_open = _safe_float(response.get("price")) or _safe_float(payload.get("price")) or 0.0
    status = response.get("status") or ""
    sl = _safe_float(response.get("sl"))
    tp = _safe_float(response.get("tp"))
    comment = response.get("comment") or payload.get("comment") or ""
    opened_at_val = response.get("opened_at")
    if isinstance(opened_at_val, str):
        try:
            opened_at = timezone.make_aware(datetime.fromisoformat(opened_at_val))
        except Exception:
            opened_at = timezone.now()
    else:
        opened_at = opened_at_val or timezone.now()

    OperationMT5Trade.objects.update_or_create(
        operation=operation,
        leg=leg,
        defaults={
            "symbol": symbol,
            "ticket": int(response.get("ticket") or 0),
            "side": (payload.get("side") or "").upper(),
            "volume": volume,
            "price_open": price_open,
            "sl": sl,
            "tp": tp,
            "comment": comment,
            "opened_at": opened_at,
            "raw_response": response,
            "status": status,
        },
    )


def _build_trade_payload(operation: Operation, role: str) -> dict[str, object]:
    if role not in {"sell", "buy"}:
        raise ValueError("role must be 'sell' or 'buy'")
    asset = operation.sell_asset if role == "sell" else operation.buy_asset
    symbol = _normalize_symbol(asset)
    if not symbol:
        raise ValueError("Asset without a valid ticker for MT5")

    quantity = operation.sell_quantity if role == "sell" else operation.buy_quantity
    price = operation.sell_price if role == "sell" else operation.buy_price
    if price is None:
        raise ValueError("Price not provided for role " + role)
    if quantity is None or quantity <= 0:
        raise ValueError("Invalid quantity for role " + role)

    volume = float(quantity)

    payload: dict[str, object] = {
        "symbol": symbol,
        "side": role,
        "lots": volume,
        "lot_size": 1,
        "quantity": int(quantity),
        "price": float(price),
        "deviation": int(getattr(settings, "MT5_TRADE_DEVIATION", 20)),
        "comment": _build_comment(operation, role),
        "type_time": "GTC",
        "type_filling": "IOC",
    }
    return payload


def execute_pair_trade(operation: Operation) -> list[dict[str, object]]:
    """Dispatch the paired sell/buy orders through the MT5 bridge."""
    logger.info("MT5: starting execute_pair_trade for operation %s", operation.pk)

    try:
        trade_contexts: list[dict[str, object]] = []
        for role in ("sell", "buy"):
            payload = _build_trade_payload(operation, role)
            request_id = uuid.uuid4()
            payload["request_id"] = str(request_id)
            event = create_mt5_audit_event(
                operation=operation,
                leg=role,
                payload=payload,
                request_id=request_id,
                action="OPEN",
                reason=MT5_OPEN_REASON,
            )
            trade_contexts.append(
                {
                    "role": role,
                    "leg_code": "A" if role == "sell" else "B",
                    "payload": payload,
                    "event": event,
                }
            )
            _log_mt5_order_event(event, "request")
        trades = [context["payload"] for context in trade_contexts]
        logger.debug(
            "MT5: payloads prepared for operation %s: %s",
            operation.pk,
            trades,
        )
    except ValueError as exc:
        logger.error("MT5: failed to build payload for operation %s: %s", operation.pk, exc)
        raise MT5TradeExecutionError(str(exc)) from exc

    try:
        logger.info("MT5: sending trades to bridge for operation %s", operation.pk)
        payload_summary = [
            {
                "symbol": ctx["payload"]["symbol"],
                "lots": ctx["payload"]["lots"],
                "quantity": ctx["payload"]["quantity"],
            }
            for ctx in trade_contexts
        ]
        logger.info(
            "MT5: final payloads (symbol/lots/quantity): %s",
            payload_summary,
        )
        if getattr(settings, "MT5_DRY_RUN", False):
            logger.info(
                "MT5: dry run mode enabled, skipping MT5 bridge for operation %s",
                operation.pk,
            )
            simulated_results: list[dict[str, object]] = []
            for context in trade_contexts:
                response = {
                    "symbol": context["payload"]["symbol"],
                    "ticket": 0,
                    "order": 0,
                    "deal": 0,
                    "position": 0,
                    "retcode": 0,
                    "price": context["payload"]["price"],
                    "volume": context["payload"]["lots"],
                    "comment": "dry-run",
                    "account_login": "",
                    "account_server": "",
                    "request_id": context["payload"].get("request_id"),
                }
                simulated_results.append(response)
                update_mt5_audit_event(context["event"], response=response)
                _log_mt5_order_event(context["event"], "response", response=response)
            logger.info(
                "MT5: dry run results for operation %s: %s",
                operation.pk,
                simulated_results,
            )
            return simulated_results
        result = execute_trades(trades)
        logger.info("MT5: bridge response for operation %s: %s", operation.pk, result)

        for idx, context in enumerate(trade_contexts):
            response = result[idx] if isinstance(result, list) and idx < len(result) else {}
            if not isinstance(response, dict):
                response = {}
            _persist_mt5_trade(operation, context["leg_code"], context["payload"], response)
            update_mt5_audit_event(context["event"], response=response)
            _log_mt5_order_event(context["event"], "response", response=response)

        return result
    except MT5BridgeError as exc:
        logger.error(
            "MT5: failed to execute orders for operation %s: %s",
            operation.pk,
            exc,
        )
        for context in trade_contexts:
            update_mt5_audit_event(
                context["event"], response=None, error_message=str(exc)
            )
            _log_mt5_order_event(
                context["event"], "error", response=None, error_message=str(exc)
            )
        raise MT5TradeExecutionError(str(exc)) from exc


def _log_mt5_order_event(
    event: MT5AuditEvent,
    stage: str,
    response: dict[str, object] | None = None,
    error_message: str | None = None,
) -> None:
    data: dict[str, object | None] = {
        "timestamp": timezone.now().isoformat(),
        "request_id": str(event.request_id),
        "operation_id": event.operation_id,
        "leg": event.leg,
        "symbol": event.symbol,
        "volume": event.volume,
        "action": event.action,
        "reason": event.reason,
        "stage": stage,
        "position_id": None,
        "order": None,
        "deal": None,
        "ticket": None,
        "retcode": None,
        "message": None,
        "login": None,
        "server": None,
        "error": error_message,
    }
    if response:
        data["position_id"] = response.get("position")
        data["order"] = response.get("order")
        data["deal"] = response.get("deal")
        data["ticket"] = response.get("ticket") or response.get("order")
        data["retcode"] = response.get("retcode")
        data["message"] = response.get("comment") or response.get("error")
        data["login"] = response.get("account_login")
        data["server"] = response.get("account_server")
    logger.info("MT5Audit %s", json.dumps(data, default=str, ensure_ascii=False))
