from __future__ import annotations

import json
from pathlib import Path
TIMEFRAME_FILE = Path("data") / "monitor_timeframe.json"
TIMEFRAME_OPTIONS = ("D1", "H1", "M15", "M5")
DEFAULT_TIMEFRAME = "M5"


def _ensure_data_dir() -> None:
    TIMEFRAME_FILE.parent.mkdir(parents=True, exist_ok=True)


def load_timeframe_setting() -> str:
    if not TIMEFRAME_FILE.exists():
        return DEFAULT_TIMEFRAME
    try:
        raw = json.loads(TIMEFRAME_FILE.read_text(encoding="utf-8"))
        value = raw.get("timeframe")
        if isinstance(value, str):
            normalized = value.strip().upper()
            if normalized in TIMEFRAME_OPTIONS:
                return normalized
    except Exception:
        pass
    return DEFAULT_TIMEFRAME


def save_timeframe_setting(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in TIMEFRAME_OPTIONS:
        raise ValueError(f"timeframe must be one of {TIMEFRAME_OPTIONS}")
    _ensure_data_dir()
    payload = {"timeframe": normalized}
    TIMEFRAME_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return normalized

