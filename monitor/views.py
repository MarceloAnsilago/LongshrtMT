from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import csv

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_http_methods
from urllib.parse import unquote

from timeframe_config import TIMEFRAME_OPTIONS, load_timeframe_setting, save_timeframe_setting

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


@require_GET
def monitor_all_pairs(request):
    csv_path = BASE_DIR / "pairs_rank.csv"
    if not csv_path.exists():
        return JsonResponse({
            "ts": now_ts(),
            "pairs": [],
            "error": "pairs_rank.csv not found",
        }, status=404)

    pairs: list[dict[str, object]] = []
    try:
        with csv_path.open("r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                pair_value = (row.get("pair") or "").strip()
                if not pair_value:
                    continue
                entry: dict[str, object] = {"pair": pair_value}
                for field in ("score", "corr", "alpha", "beta", "half_life"):
                    raw = row.get(field)
                    if raw is None or raw == "":
                        entry[field] = None
                    else:
                        try:
                            entry[field] = float(raw)
                        except ValueError:
                            entry[field] = raw
                pairs.append(entry)
    except Exception as exc:
        return JsonResponse({
            "ts": now_ts(),
            "pairs": [],
            "error": str(exc),
        }, status=500)

    return JsonResponse({
        "ts": now_ts(),
        "pairs": pairs,
    })


@require_http_methods(["GET", "POST"])
def monitor_timeframe(request):
    if request.method == "GET":
        return JsonResponse({
            "timeframe": load_timeframe_setting(),
            "options": list(TIMEFRAME_OPTIONS),
            "ts": now_ts(),
        })

    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError as exc:
        return JsonResponse({"error": f"invalid json: {exc}"}, status=400)

    timeframe = data.get("timeframe")
    if not isinstance(timeframe, str):
        return JsonResponse({"error": "timeframe is required"}, status=400)

    try:
        saved = save_timeframe_setting(timeframe)
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    return JsonResponse({"timeframe": saved})


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
