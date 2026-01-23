import uuid
from types import SimpleNamespace

import httpx
from django.conf import settings
from django.db import IntegrityError, transaction
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from .models import Mt5Terminal, OrderEvent, OrderRequest


FINAL_STATUSES = {
    OrderRequest.Status.FILLED,
    OrderRequest.Status.REJECTED,
    OrderRequest.Status.CANCELLED,
    OrderRequest.Status.EXPIRED,
}


def _supabase_enabled() -> bool:
    return bool(getattr(settings, "SUPABASE_URL", "") and getattr(settings, "SUPABASE_SERVICE_KEY", ""))


def _supabase_rest_url() -> str:
    base = getattr(settings, "SUPABASE_URL", "").rstrip("/")
    return f"{base}/rest/v1"


def _supabase_headers(prefer: str | None = "return=representation") -> dict[str, str]:
    key = getattr(settings, "SUPABASE_SERVICE_KEY", "")
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    return headers


def _supabase_request(method: str, url: str, *, json_body=None, params=None) -> httpx.Response:
    timeout = httpx.Timeout(10.0)
    return httpx.request(method, url, headers=_supabase_headers(), json=json_body, params=params, timeout=timeout)


def get_terminal_status(terminal_id: str) -> dict | None:
    if not _supabase_enabled():
        try:
            terminal = Mt5Terminal.objects.get(pk=terminal_id)
        except Mt5Terminal.DoesNotExist:
            return None
        return {
            "terminal_id": terminal.terminal_id,
            "online": is_terminal_online(terminal_id),
            "last_seen_at": terminal.last_seen_at,
            "status": terminal.status,
            "meta": terminal.meta,
        }

    url = f"{_supabase_rest_url()}/bridge_mt5terminal"
    resp = _supabase_request(
        "GET",
        url,
        params={"terminal_id": f"eq.{terminal_id}", "select": "terminal_id,last_seen_at,status,meta"},
    )
    if resp.status_code >= 300:
        return None
    data = resp.json() or []
    if not data:
        return None
    row = data[0]
    last_seen = parse_datetime(row.get("last_seen_at") or "")
    online = False
    if last_seen:
        delta = timezone.now() - last_seen
        online = delta.total_seconds() <= 15
    return {
        "terminal_id": row.get("terminal_id"),
        "online": online,
        "last_seen_at": last_seen,
        "status": row.get("status"),
        "meta": row.get("meta"),
    }


def touch_terminal(terminal_id: str, meta: dict | None = None) -> dict | None:
    if not _supabase_enabled():
        terminal, _ = Mt5Terminal.objects.get_or_create(terminal_id=terminal_id)
        terminal.last_seen_at = timezone.now()
        terminal.status = "online"
        if isinstance(meta, dict):
            terminal.meta = meta
        terminal.save(update_fields=["last_seen_at", "status", "meta"])
        return {
            "terminal_id": terminal.terminal_id,
            "status": terminal.status,
            "last_seen_at": terminal.last_seen_at,
        }

    url = f"{_supabase_rest_url()}/bridge_mt5terminal"
    payload = {
        "terminal_id": terminal_id,
        "last_seen_at": timezone.now().isoformat(),
        "status": "online",
    }
    if isinstance(meta, dict):
        payload["meta"] = meta
    resp = _supabase_request(
        "POST",
        url,
        json_body=payload,
        params={"on_conflict": "terminal_id"},
    )
    if resp.status_code >= 300:
        return None
    data = resp.json() or []
    return data[0] if data else None


def is_terminal_online(terminal_id: str, max_age_seconds: int = 15) -> bool:
    if not _supabase_enabled():
        try:
            terminal = Mt5Terminal.objects.get(pk=terminal_id)
        except Mt5Terminal.DoesNotExist:
            return False
        delta = timezone.now() - terminal.last_seen_at
        return delta.total_seconds() <= max_age_seconds

    status = get_terminal_status(terminal_id)
    if not status:
        return False
    last_seen = status.get("last_seen_at")
    if not last_seen:
        return False
    delta = timezone.now() - last_seen
    return delta.total_seconds() <= max_age_seconds


def create_order(
    terminal_id: str,
    pair_id: str,
    side: str,
    symbol_a: str,
    qty_a,
    symbol_b=None,
    qty_b=None,
    order_type: str = OrderRequest.OrderType.MARKET,
) -> OrderRequest:
    if _supabase_enabled():
        url = f"{_supabase_rest_url()}/bridge_orderrequest"
        for _ in range(5):
            client_order_id = uuid.uuid4().hex
            payload = {
                "terminal_id": terminal_id,
                "pair_id": pair_id,
                "side": side,
                "symbol_a": symbol_a,
                "qty_a": qty_a,
                "symbol_b": symbol_b,
                "qty_b": qty_b,
                "order_type": order_type,
                "status": OrderRequest.Status.QUEUED,
                "client_order_id": client_order_id,
            }
            resp = _supabase_request(
                "POST",
                url,
                json_body=payload,
                params={"on_conflict": "terminal_id,client_order_id"},
            )
            if resp.status_code == 409:
                continue
            if resp.status_code < 300:
                data = resp.json() or []
                if data:
                    return SimpleNamespace(**data[0])
                continue
        raise IntegrityError("Unable to generate unique client_order_id")

    terminal, _ = Mt5Terminal.objects.get_or_create(
        terminal_id=terminal_id,
        defaults={"status": "online"},
    )

    for _ in range(5):
        client_order_id = uuid.uuid4().hex
        try:
            return OrderRequest.objects.create(
                terminal=terminal,
                pair_id=pair_id,
                side=side,
                symbol_a=symbol_a,
                qty_a=qty_a,
                symbol_b=symbol_b,
                qty_b=qty_b,
                order_type=order_type,
                status=OrderRequest.Status.QUEUED,
                client_order_id=client_order_id,
            )
        except IntegrityError:
            continue
    raise IntegrityError("Unable to generate unique client_order_id")


def claim_next_order(terminal_id: str) -> OrderRequest | None:
    if _supabase_enabled():
        # No RPC in Django bridge tables; use DB flow (Django) instead.
        return None

    now = timezone.now()
    with transaction.atomic():
        candidate_ids = list(
            OrderRequest.objects.filter(
                terminal_id=terminal_id,
                status=OrderRequest.Status.QUEUED,
            )
            .order_by("created_at")
            .values_list("id", flat=True)[:3]
        )
        for order_id in candidate_ids:
            updated = OrderRequest.objects.filter(
                id=order_id,
                status=OrderRequest.Status.QUEUED,
            ).update(
                status=OrderRequest.Status.CLAIMED,
                claimed_at=now,
            )
            if updated:
                return OrderRequest.objects.select_related("terminal").get(id=order_id)
    return None


def update_order_status(
    order_id,
    status: str,
    payload: dict | None = None,
    error: str | None = None,
    done: bool = False,
    event_type: str | None = None,
) -> OrderRequest:
    if _supabase_enabled():
        url = f"{_supabase_rest_url()}/bridge_orderrequest"
        update_payload = {"status": status}
        if error is not None:
            update_payload["error"] = error
        if done or status in FINAL_STATUSES:
            update_payload["done_at"] = timezone.now().isoformat()
        resp = _supabase_request(
            "PATCH",
            url,
            json_body=update_payload,
            params={"id": f"eq.{order_id}"},
        )
        if payload is not None:
            _supabase_request(
                "POST",
                f"{_supabase_rest_url()}/bridge_orderevent",
                json_body={
                    "order_id": str(order_id),
                    "event_type": event_type or OrderEvent.EventType.INFO,
                    "payload": payload,
                },
            )
        if resp.status_code < 300:
            data = resp.json() or []
            if data:
                return SimpleNamespace(**data[0])
        return SimpleNamespace(id=order_id)

    order = OrderRequest.objects.get(pk=order_id)
    update_fields = ["status"]
    order.status = status
    if error is not None:
        order.error = error
        update_fields.append("error")
    if done or status in FINAL_STATUSES:
        order.done_at = timezone.now()
        update_fields.append("done_at")
    order.save(update_fields=update_fields)

    if payload is not None:
        OrderEvent.objects.create(
            order=order,
            event_type=event_type or OrderEvent.EventType.INFO,
            payload=payload,
        )
    return order
