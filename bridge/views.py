from __future__ import annotations

import json

from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from .models import OrderRequest
from .services import claim_next_order, create_order, get_terminal_status, touch_terminal, update_order_status


def _parse_json_payload(request) -> dict:
    if not request.body:
        return {}
    try:
        return json.loads(request.body.decode("utf-8"))
    except json.JSONDecodeError:
        return {}


def _require_ea_token(request):
    expected = getattr(settings, "EA_SHARED_TOKEN", "")
    token = request.headers.get("X-EA-TOKEN") or request.META.get("HTTP_X_EA_TOKEN")
    if not expected or token != expected:
        return JsonResponse({"ok": False, "detail": "unauthorized"}, status=403)
    return None


@require_GET
def terminal_status(request, terminal_id: str):
    status = get_terminal_status(terminal_id)
    if not status:
        return JsonResponse({"ok": False, "detail": "terminal not found"}, status=404)
    return JsonResponse(
        {
            "terminal_id": status.get("terminal_id"),
            "online": bool(status.get("online")),
            "last_seen_at": status.get("last_seen_at").isoformat() if status.get("last_seen_at") else None,
            "status": status.get("status"),
        }
    )


@csrf_exempt
@require_POST
def terminal_heartbeat(request, terminal_id: str):
    auth_error = _require_ea_token(request)
    if auth_error:
        return auth_error
    payload = _parse_json_payload(request)
    meta = payload.get("meta") if isinstance(payload, dict) else None
    info = touch_terminal(terminal_id, meta=meta if isinstance(meta, dict) else None)
    if not info:
        return JsonResponse({"ok": False, "detail": "terminal update failed"}, status=500)
    return JsonResponse({"ok": True, "terminal_id": terminal_id})


@csrf_exempt
@require_POST
def orders_next(request):
    auth_error = _require_ea_token(request)
    if auth_error:
        return auth_error
    terminal_id = request.GET.get("terminal_id")
    if not terminal_id:
        return JsonResponse({"ok": False, "detail": "terminal_id required"}, status=400)
    order = claim_next_order(terminal_id)
    if not order:
        return HttpResponse(status=204)
    return JsonResponse(
        {
            "id": str(order.id),
            "client_order_id": order.client_order_id,
            "pair_id": order.pair_id,
            "side": order.side,
            "symbol_a": order.symbol_a,
            "symbol_b": order.symbol_b,
            "qty_a": str(order.qty_a),
            "qty_b": str(order.qty_b) if order.qty_b is not None else None,
            "order_type": order.order_type,
            "created_at": order.created_at.isoformat(),
        }
    )


@csrf_exempt
@require_POST
def orders_test(request):
    auth_error = _require_ea_token(request)
    if auth_error:
        return auth_error
    payload = _parse_json_payload(request)

    terminal_id = payload.get("terminal_id") or getattr(settings, "MT5_DEFAULT_TERMINAL", "VPS01")
    symbol_a = (payload.get("symbol_a") or "").strip()
    qty_a = payload.get("qty_a")
    symbol_b = (payload.get("symbol_b") or "").strip() or None
    qty_b = payload.get("qty_b")
    side = (payload.get("side") or OrderRequest.Side.BUY).upper()
    order_type = (payload.get("order_type") or OrderRequest.OrderType.MARKET).upper()
    pair_id = payload.get("pair_id") or (f"{symbol_a}_{symbol_b}" if symbol_b else symbol_a)

    if not symbol_a or qty_a in (None, "", 0):
        return JsonResponse({"ok": False, "detail": "symbol_a and qty_a required"}, status=400)

    order = create_order(
        terminal_id=terminal_id,
        pair_id=pair_id,
        side=side,
        symbol_a=symbol_a,
        qty_a=qty_a,
        symbol_b=symbol_b,
        qty_b=qty_b,
        order_type=order_type,
    )
    order_id = getattr(order, "id", None)
    return JsonResponse(
        {
            "ok": True,
            "order_id": str(order_id) if order_id is not None else None,
            "terminal_id": terminal_id,
            "status": OrderRequest.Status.QUEUED,
        }
    )


@csrf_exempt
@require_POST
def order_ack(request, order_id):
    auth_error = _require_ea_token(request)
    if auth_error:
        return auth_error
    payload = _parse_json_payload(request)
    update_order_status(
        order_id=order_id,
        status=OrderRequest.Status.SENT,
        payload=payload,
        event_type="ACK",
    )
    return JsonResponse({"ok": True})


@csrf_exempt
@require_POST
def order_fill(request, order_id):
    auth_error = _require_ea_token(request)
    if auth_error:
        return auth_error
    payload = _parse_json_payload(request)
    update_order_status(
        order_id=order_id,
        status=OrderRequest.Status.FILLED,
        payload=payload,
        done=True,
        event_type="FILL",
    )
    return JsonResponse({"ok": True})


@csrf_exempt
@require_POST
def order_reject(request, order_id):
    auth_error = _require_ea_token(request)
    if auth_error:
        return auth_error
    payload = _parse_json_payload(request)
    error = payload.get("error") if isinstance(payload, dict) else None
    update_order_status(
        order_id=order_id,
        status=OrderRequest.Status.REJECTED,
        payload=payload,
        error=error,
        done=True,
        event_type="ERROR",
    )
    return JsonResponse({"ok": True})
