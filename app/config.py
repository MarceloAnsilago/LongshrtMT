import os
from dataclasses import dataclass


@dataclass
class Settings:
    # monitor
    top_n: int = 5
    poll_seconds: float = 1.0
    z_window: int = 120
    warmup: int = 20
    entry_confirm_mode: str = os.getenv("ENTRY_CONFIRM_MODE", "none").strip().lower()
    sigma_min: float = float(os.getenv("SIGMA_MIN", "0") or 0.0)

    stale_seconds: float = 300.0

    # strategy
    enter_z: float = 2.1
    exit_band: float = 0.2

    # files
    pairs_csv: str = "pairs_rank.csv"
    trades_csv: str = "data/trades_multi.csv"


SETTINGS = Settings()
