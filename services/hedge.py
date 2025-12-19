from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal, Optional, List

import MetaTrader5 as mt5

PriceField = Literal["close", "open", "high", "low"]
Mode = Literal["price", "log"]


@dataclass
class HedgeOLS:
    alpha: float
    beta: float
    n: int
    corr: float


def _corr(x: List[float], y: List[float]) -> float:
    n = len(x)
    if n < 2:
        return 0.0
    mx = sum(x) / n
    my = sum(y) / n
    vx = sum((v - mx) ** 2 for v in x) / n
    vy = sum((v - my) ** 2 for v in y) / n
    if vx == 0 or vy == 0:
        return 0.0
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n)) / n
    return cov / math.sqrt(vx * vy)


def _ols_with_intercept(y: List[float], x: List[float]) -> tuple[float, float]:
    """
    Regressão OLS: y = alpha + beta*x
    beta = cov(x,y)/var(x)
    alpha = mean(y) - beta*mean(x)
    """
    n = len(x)
    mx = sum(x) / n
    my = sum(y) / n
    varx = sum((v - mx) ** 2 for v in x) / n
    if varx == 0:
        return 0.0, 0.0
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n)) / n
    beta = cov / varx
    alpha = my - beta * mx
    return alpha, beta


def estimate_ols_from_mt5(
    sym_a: str,
    sym_b: str,
    timeframe=mt5.TIMEFRAME_D1,
    bars: int = 300,
    price_field: PriceField = "close",
    mode: Mode = "log",
) -> Optional[HedgeOLS]:
    """
    Estima alpha/beta por OLS:
      A = alpha + beta*B

    mode="log" usa log(preço) (recomendado para estabilidade).
    """

    if not mt5.initialize():
        print("❌ MT5 init falhou:", mt5.last_error())
        return None

    try:
        ra = mt5.copy_rates_from_pos(sym_a, timeframe, 0, bars)
        rb = mt5.copy_rates_from_pos(sym_b, timeframe, 0, bars)

        if ra is None or rb is None:
            print("❌ Falha ao puxar rates:", mt5.last_error())
            return None

        n = min(len(ra), len(rb))
        if n < 80:
            print(f"⚠️ Poucos candles (n={n}) para estimar OLS com segurança.")
            return None

        a = [float(ra[i][price_field]) for i in range(n)]
        b = [float(rb[i][price_field]) for i in range(n)]

        if mode == "log":
            a = [math.log(v) for v in a if v > 0]
            b = [math.log(v) for v in b if v > 0]
            n = min(len(a), len(b))
            a = a[:n]
            b = b[:n]
        else:
            a = a[:n]
            b = b[:n]

        corr = _corr(a, b)
        alpha, beta = _ols_with_intercept(a, b)

        return HedgeOLS(alpha=alpha, beta=beta, n=n, corr=corr)

    finally:
        mt5.shutdown()


if __name__ == "__main__":
    # Use D1 pra beta (mais estável). Ajuste bars se quiser.
    res = estimate_ols_from_mt5("PETR4", "VALE3", timeframe=mt5.TIMEFRAME_D1, bars=400, mode="log")
    print(res)
