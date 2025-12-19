from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET
from urllib.parse import unquote

BASE_DIR = Path(__file__).resolve().parent.parent
STATE_PATH = BASE_DIR / "data" / "monitor_state.json"
HISTORY_PATH = BASE_DIR / "data" / "monitor_history.json"
HISTORY_DEFAULT_MAXLEN = 600


def now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


@require_GET
def monitor_state(request):
    if not STATE_PATH.exists():
        return JsonResponse({"ts": now_ts(), "pairs": [], "error": "state file missing"})
    try:
        payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        return JsonResponse({"ts": now_ts(), "pairs": [], "error": str(exc)})
    return JsonResponse(payload)


def _load_history_data() -> dict[str, object]:
    if not HISTORY_PATH.exists():
        return {"ts": now_ts(), "params": {}, "history": {}}
    try:
        raw = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("invalid history payload")
        history = raw.get("history")
        if not isinstance(history, dict):
            raw["history"] = {}
        return raw
    except Exception:
        return {"ts": now_ts(), "params": {}, "history": {}}


@require_GET
def monitor_history_api(request):
    pair = request.GET.get("pair")
    if not pair:
        return JsonResponse({"error": "pair parameter is required"}, status=400)
    limit_param = request.GET.get("limit")
    try:
        limit = int(limit_param) if limit_param is not None else HISTORY_DEFAULT_MAXLEN
    except ValueError:
        limit = HISTORY_DEFAULT_MAXLEN
    limit = max(1, min(limit, HISTORY_DEFAULT_MAXLEN))

    history_data = _load_history_data()
    if not HISTORY_PATH.exists():
        return JsonResponse({
            "pair": pair,
            "ts": history_data.get("ts"),
            "params": history_data.get("params", {}),
            "points": [],
        })

    history_map = history_data.get("history", {})
    points = history_map.get(pair)
    if points is None:
        return JsonResponse({"error": "pair not found"}, status=404)

    sliced = points[-limit:] if limit < len(points) else points
    return JsonResponse({
        "pair": pair,
        "ts": history_data.get("ts"),
        "params": history_data.get("params", {}),
        "points": sliced,
    })


@require_GET
def dashboard(request):
    return render(request, "monitor/dashboard.html")


@require_GET
def pair_view(request, pair):
    decoded = unquote(pair)
    return render(request, "monitor/pair_chart.html", {"pair": decoded})
