from __future__ import annotations
from dataclasses import dataclass
import math
import numpy as np

from services.mt5_connect import MT5Service


@dataclass
class CalibResult:
    alpha: float
    beta: float
    corr: float
    n: int


def ols_alpha_beta(y: np.ndarray, x: np.ndarray) -> CalibResult:
    # y = alpha + beta*x + e
    x = x.astype(float)
    y = y.astype(float)

    x_mean = x.mean()
    y_mean = y.mean()

    cov = np.mean((x - x_mean) * (y - y_mean))
    var = np.mean((x - x_mean) ** 2)
    beta = cov / var if var > 0 else 1.0
    alpha = y_mean - beta * x_mean

    corr = float(np.corrcoef(y, x)[0, 1]) if len(y) > 2 else 0.0
    return CalibResult(alpha=float(alpha), beta=float(beta), corr=corr, n=len(y))


def calibrate_pair_from_mt5(mt5: MT5Service, a: str, b: str, bars: int = 200):
    ca = mt5.get_close_series_d1(a, bars=bars, shift=1)
    cb = mt5.get_close_series_d1(b, bars=bars, shift=1)

    n = min(len(ca), len(cb))
    if n < 50:
        raise RuntimeError(f"series curta demais: {a}={len(ca)} {b}={len(cb)}")

    y = np.array(ca[-n:], dtype=float)
    x = np.array(cb[-n:], dtype=float)
    y = np.array([math.log(v) for v in y], dtype=float)
    x = np.array([math.log(v) for v in x], dtype=float)
    return ols_alpha_beta(y, x)


def calibrate_pair_from_series(ca: list[float], cb: list[float]) -> CalibResult:
    n = min(len(ca), len(cb))
    if n < 50:
        raise RuntimeError(f"series curta demais: {len(ca)} {len(cb)}")

    ca_trim = [float(v) for v in ca[-n:]]
    cb_trim = [float(v) for v in cb[-n:]]

    if any(v <= 0 for v in ca_trim + cb_trim):
        raise RuntimeError("series contem precos invalidos (<= 0)")

    y = np.array([math.log(v) for v in ca_trim], dtype=float)
    x = np.array([math.log(v) for v in cb_trim], dtype=float)
    return ols_alpha_beta(y, x)
