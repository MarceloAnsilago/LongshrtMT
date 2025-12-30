from __future__ import annotations

import uuid
from typing import Any, Dict, List

from operacoes.models import MT5AuditEvent, Operation


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def create_mt5_audit_event(
    operation: Operation,
    leg: str,
    payload: Dict[str, Any],
    request_id: uuid.UUID,
    action: str = "OPEN",
    reason: str = "strategy_entry",
) -> MT5AuditEvent:
    volume = _safe_float(payload.get("lots") or payload.get("quantity"))
    return MT5AuditEvent.objects.create(
        request_id=request_id,
        operation=operation,
        leg=leg,
        symbol=str(payload.get("symbol") or ""),
        volume=volume,
        action=action,
        reason=reason,
        request_payload=dict(payload),
    )


def update_mt5_audit_event(
    event: MT5AuditEvent,
    response: Dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    fields: List[str] = []
    if response is not None:
        event.response_payload = response
        fields.append("response_payload")

        _assign_field(event, "retcode", _safe_int(response.get("retcode")), fields)
        _assign_field(
            event,
            "order",
            _safe_int(response.get("order") or response.get("ticket")),
            fields,
        )
        _assign_field(event, "ticket", _safe_int(response.get("ticket")), fields)
        _assign_field(
            event,
            "deal",
            _safe_int(response.get("deal")),
            fields,
        )
        _assign_field(
            event,
            "position_id",
            _safe_int(response.get("position")),
            fields,
        )
        _assign_field(
            event,
            "account_login",
            str(response.get("account_login") or ""),
            fields,
        )
        _assign_field(
            event,
            "account_server",
            str(response.get("account_server") or ""),
            fields,
        )

    if error_message is not None:
        event.error_message = error_message
        fields.append("error_message")

    if fields:
        event.save(update_fields=fields + ["updated_at"])
    else:
        event.save(update_fields=["updated_at"])


def _assign_field(
    event: MT5AuditEvent,
    field_name: str,
    value: Any,
    fields: List[str],
) -> None:
    if value is None:
        return
    setattr(event, field_name, value)
    fields.append(field_name)
