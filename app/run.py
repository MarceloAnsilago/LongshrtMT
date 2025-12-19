from __future__ import annotations

import os

from services.multi_monitor import main as monitor_main
from app.config import SETTINGS


def main():
    print("APP.RUN FILE =", os.path.abspath(__file__))
    monitor_main(
        top_n=SETTINGS.top_n,
        enter_z=SETTINGS.enter_z,
        exit_band=SETTINGS.exit_band,
        z_window=SETTINGS.z_window,
        warmup=SETTINGS.warmup,
        poll_seconds=SETTINGS.poll_seconds,
        stale_seconds=SETTINGS.stale_seconds,
        entry_confirm_mode=SETTINGS.entry_confirm_mode,
        sigma_min=SETTINGS.sigma_min,
    )


if __name__ == "__main__":
    main()
