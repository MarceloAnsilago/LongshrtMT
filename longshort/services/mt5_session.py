from __future__ import annotations

import atexit
import logging

import MetaTrader5 as mt5

logger = logging.getLogger(__name__)

_mt5_initialized = False


def _reset_state() -> None:
    global _mt5_initialized
    _mt5_initialized = False


def ensure_mt5_initialized() -> bool:
    global _mt5_initialized
    if _mt5_initialized:
        return True
    try:
        initialized = mt5.initialize()
    except Exception as exc:
        logger.error("MT5 initialization raised: %s", exc)
        initialized = False
    if initialized:
        _mt5_initialized = True
        return True
    err = mt5.last_error()
    logger.error("MT5 initialize failed: %s", err)
    return False


def shutdown_mt5() -> None:
    global _mt5_initialized
    if not _mt5_initialized:
        return
    try:
        mt5.shutdown()
    except Exception:
        logger.exception("Error shutting down MT5")
    finally:
        _reset_state()


atexit.register(shutdown_mt5)
