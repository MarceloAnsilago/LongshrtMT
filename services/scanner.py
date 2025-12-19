from __future__ import annotations

import csv
import math
import shutil
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from time import sleep
from typing import List, Optional

from statsmodels.tsa.stattools import adfuller

import MetaTrader5 as mt5

BASE_BARS = 180
C30_TAIL = 30
C60_TAIL = 60
MIN_CORR = 0.70
MIN_CORR30 = 0.60
MIN_CORR60 = 0.70
MAX_ADF = 0.10


@dataclass
class OLS:
    alpha: float
    beta: float
    corr: float
    n: int


@dataclass
class PairScanResult:
    a: str
    b: str

    # base
    corr: float
    corr30: float
    corr60: float

    alpha: float
    beta: float

    resid_std: float
    half_life: float
    adf_p: float

    score: float


def _corr(x: List[float], y: List[float]) -> float:
    n = len(x)
    mx = sum(x) / n
    my = sum(y) / n
    vx = sum((v - mx) ** 2 for v in x) / n
    vy = sum((v - my) ** 2 for v in y) / n
    if vx == 0 or vy == 0:
        return 0.0
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n)) / n
    return cov / math.sqrt(vx * vy)


def corr_tail(x: list[float], y: list[float], tail: int) -> float:
    n = min(len(x), len(y))
    if n < tail:
        return 0.0
    return _corr(x[-tail:], y[-tail:])


def adf_pvalue(resid: list[float]) -> Optional[float]:
    """
    ADF no resíduo. Retorna p-valor.
    Usamos autolag='AIC' e regressão com constante.
    """
    if len(resid) < 120:
        return None
    try:
        out = adfuller(resid, autolag="AIC", regression="c")
        return float(out[1])
    except Exception:
        return None


def _ols_with_intercept(y: List[float], x: List[float]) -> tuple[float, float]:
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


def _rates(symbol: str, timeframe, bars: int, tries: int = 6):
    # garante que o símbolo está selecionado
    mt5.symbol_select(symbol, True)

    for _ in range(tries):
        r = mt5.copy_rates_from_pos(symbol, timeframe, 0, bars)
        if r is not None and len(r) > max(80, bars // 5):
            return r
        sleep(0.2)
    return None


def estimate_ols_log(sym_a: str, sym_b: str, timeframe, bars: int) -> Optional[OLS]:
    ra = _rates(sym_a, timeframe, bars)
    rb = _rates(sym_b, timeframe, bars)
    if ra is None or rb is None:
        return None

    n = min(len(ra), len(rb))
    if n < 120:
        return None

    a = [float(ra[i]["close"]) for i in range(n)]
    b = [float(rb[i]["close"]) for i in range(n)]

    # log-preço
    a = [math.log(v) for v in a if v > 0]
    b = [math.log(v) for v in b if v > 0]
    n = min(len(a), len(b))
    if n < 120:
        return None

    a = a[:n]
    b = b[:n]

    corr = _corr(a, b)
    alpha, beta = _ols_with_intercept(a, b)
    return OLS(alpha=alpha, beta=beta, corr=corr, n=n)


def resid_series_log(sym_a: str, sym_b: str, ols: OLS, timeframe, bars: int) -> Optional[list[float]]:
    ra = _rates(sym_a, timeframe, bars)
    rb = _rates(sym_b, timeframe, bars)
    if ra is None or rb is None:
        return None

    n = min(len(ra), len(rb))
    if n < 120:
        return None

    a = [math.log(float(ra[i]["close"])) for i in range(n) if float(ra[i]["close"]) > 0]
    b = [math.log(float(rb[i]["close"])) for i in range(n) if float(rb[i]["close"]) > 0]
    n = min(len(a), len(b))
    if n < 120:
        return None

    a = a[:n]
    b = b[:n]

    resid = [a[i] - (ols.alpha + ols.beta * b[i]) for i in range(n)]
    return resid


def half_life_from_resid(resid: list[float]) -> Optional[float]:
    """
    Estima half-life via: Δe_t = a + b * e_{t-1}
    half-life = -ln(2) / b  (quando b < 0)
    """
    if len(resid) < 120:
        return None

    x = resid[:-1]                # e_{t-1}
    y = [resid[i+1] - resid[i] for i in range(len(resid)-1)]  # Δe_t

    n = len(x)
    mx = sum(x) / n
    my = sum(y) / n

    varx = sum((v - mx) ** 2 for v in x) / n
    if varx == 0:
        return None

    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n)) / n
    b = cov / varx

    if b >= 0:
        return None  # não reverte (ou muito fraco)

    hl = -math.log(2) / b
    # limita pra evitar explosões numéricas
    if hl <= 0 or hl > 10_000:
        return None
    return hl


def log_series(symbol: str, timeframe, bars: int = BASE_BARS) -> Optional[list[float]]:
    r = _rates(symbol, timeframe, bars)
    if r is None:
        return None
    closes = [float(rr["close"]) for rr in r]
    closes = [math.log(v) for v in closes if v > 0]
    return closes if len(closes) >= 120 else None


def scan_pairs(
    symbols: List[str],
    timeframe=mt5.TIMEFRAME_D1,
    bars_ols: int = BASE_BARS,
    bars_resid: int = BASE_BARS,
    min_corr: float = MIN_CORR,
    top_n: int = 20,
) -> List[PairScanResult]:
    if not mt5.initialize():
        print("mt5.initialize falhou:", mt5.last_error())
        return []

    results: List[PairScanResult] = []
    try:
        for a, b in combinations(symbols, 2):
            ols = estimate_ols_log(a, b, timeframe, bars_ols)
            if not ols:
                continue

            if abs(ols.corr) < min_corr:
                continue

            # série do resíduo (para std/hl/adf)
            resid = resid_series_log(a, b, ols, timeframe, bars_resid)
            if resid is None:
                continue

            m = sum(resid) / len(resid)
            var = sum((r - m) ** 2 for r in resid) / len(resid)
            rs = math.sqrt(var)
            if rs == 0:
                continue

            hl = half_life_from_resid(resid)
            if hl is None:
                continue

            # ADF p-valor no resíduo
            pval = adf_pvalue(resid)
            if pval is None:
                continue

            # Séries log para corr 30/60 (janelas em D1: 30 e 60)
            la = log_series(a, timeframe, bars_ols)
            lb = log_series(b, timeframe, bars_ols)
            if la is None or lb is None:
                continue
            n = min(len(la), len(lb))
            la = la[-n:]
            lb = lb[-n:]

            c30 = corr_tail(la, lb, C30_TAIL)
            c60 = corr_tail(la, lb, C60_TAIL)

            # filtros produtivos (evita par “quebrando” no curto prazo)
            if abs(c30) < MIN_CORR30 or abs(c60) < MIN_CORR60:
                continue

            # filtro ADF (cointegração): 5% é padrão. Pode usar 10% se quiser mais sinais.
            if pval > MAX_ADF:
                continue

            # score: favorece corr, penaliza volatilidade e half-life e p-valor alto
            score = abs(ols.corr) / (rs * (1.0 + hl) * (1.0 + pval))

            results.append(PairScanResult(
                a=a, b=b,
                corr=ols.corr,
                corr30=c30,
                corr60=c60,
                alpha=ols.alpha,
                beta=ols.beta,
                resid_std=rs,
                half_life=hl,
                adf_p=pval,
                score=score
            ))

        results.sort(key=lambda r: r.score, reverse=True)
        return results[:top_n]

    finally:
        mt5.shutdown()


if __name__ == "__main__":
    universe = [
        "ITUB4", "BBDC4", "BBAS3", "ABEV3",
        "PETR4", "PETR3", "VALE3", "WEGE3",
        "B3SA3", "RENT3", "SUZB3",
        "RADL3", "LREN3", "GGBR4", "CSNA3",
    ]

    top = scan_pairs(universe, timeframe=mt5.TIMEFRAME_D1, min_corr=0.70, top_n=20)

    print("\n=== TOP PARES (corr>=0.70 + corr30/60 + ADF) ===")
    for i, r in enumerate(top, 1):
        print(
            f"{i:02d}) {r.a}/{r.b} | score={r.score:8.3f} | corr={r.corr: .3f} "
            f"| c30={r.corr30: .3f} c60={r.corr60: .3f} | beta={r.beta: .3f} "
            f"| std={r.resid_std: .6f} | hl={r.half_life: .1f} | adf_p={r.adf_p: .3f}"
        )

    with open("pairs_rank.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            ["rank", "pair", "score", "corr", "corr30", "corr60", "alpha", "beta", "resid_std", "half_life", "adf_p"]
        )
        for i, r in enumerate(top, 1):
            w.writerow([i, f"{r.a}/{r.b}", r.score, r.corr, r.corr30, r.corr60, r.alpha, r.beta, r.resid_std, r.half_life, r.adf_p])

    
    Path("data").mkdir(exist_ok=True)

    if top:
        shutil.copyfile("pairs_rank.csv", "data/pairs_last_good.csv")
        print("Cache salvo: data/pairs_last_good.csv")
    else:
        print("Scanner sem pares. Mantendo cache anterior (se existir).")

    print("\nSalvo: pairs_rank.csv")
