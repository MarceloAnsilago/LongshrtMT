from __future__ import annotations

import argparse
import csv
import itertools
import math
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import MetaTrader5 as mt5

from services.mt5_connect import MT5Config, MT5Service
from services.pair_calibration import CalibResult, calibrate_pair_from_mt5
from services.universe import UNIVERSE


DEFAULT_MIN_CORR = 0.75
DEFAULT_PRE_CORR_BUFFER = 0.05
DEFAULT_BARS = 200
DEFAULT_MAX_PAIRS = 30
MIN_RETURNS = 30


@dataclass
class PairScore:
    a: str
    b: str
    alpha: float
    beta: float
    corr: float
    half_life: float
    score: float


def correlation(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    if var_x <= 0 or var_y <= 0:
        return None
    return cov / math.sqrt(var_x * var_y)


def compute_returns(closes: list[float]) -> list[float]:
    return [math.log(curr / prev) for prev, curr in zip(closes, closes[1:]) if prev > 0 and curr > 0]


def collect_closes(service: MT5Service, symbol: str, bars: int) -> Optional[list[float]]:
    try:
        if not service.ensure_symbol(symbol):
            print(f"[{symbol}] símbolo indisponível para copy_rates", file=sys.stderr)
            return None
        rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_D1, 0, bars)
    except Exception as exc:
        print(f"[{symbol}] copy_rates falhou: {exc}", file=sys.stderr)
        return None
    if rates is None or len(rates) < bars:
        got = 0 if rates is None else len(rates)
        print(f"[{symbol}] copy_rates vazio/curto: got={got}", file=sys.stderr)
        return None
    closes = rates["close"].astype(float)
    closes = [float(v) for v in closes if v is not None and v > 0]
    if len(closes) < bars:
        print(f"[{symbol}] poucos closes válidos ({len(closes)} < {bars})", file=sys.stderr)
        return None
    return closes


def prefilter_pairs(
    returns_cache: dict[str, list[float]],
    min_corr: float,
    pre_corr_buffer: float,
) -> Iterable[tuple[str, str]]:
    symbols = sorted(returns_cache.keys())
    precorr_limit = max(abs(min_corr) - pre_corr_buffer, 0.0)
    for a, b in itertools.combinations(symbols, 2):
        ret_a = returns_cache[a]
        ret_b = returns_cache[b]
        if len(ret_a) < MIN_RETURNS or len(ret_b) < MIN_RETURNS:
            print(f"[{a}/{b}] séries muito curtas ({len(ret_a)},{len(ret_b)})")
            continue
        min_len = min(len(ret_a), len(ret_b))
        corr = correlation(ret_a[:min_len], ret_b[:min_len])
        if corr is None:
            print(f"[{a}/{b}] pré-corr indisponível")
            continue
        if abs(corr) < precorr_limit:
            print(f"[{a}/{b}] pré-corr {corr:.3f} abaixo de {precorr_limit:.3f}")
            continue
        yield a, b


def calibrate_pairs(
    service: MT5Service,
    pairs: Iterable[tuple[str, str]],
    bars: int,
    min_corr: float,
    total_pairs: int,
) -> List[PairScore]:
    out: list[PairScore] = []
    tested = 0
    kept = 0
    for idx, (a, b) in enumerate(pairs, start=1):
        tested += 1
        try:
            result: CalibResult = calibrate_pair_from_mt5(service, a, b, bars=bars)
        except Exception as exc:
            print(f"[{a}/{b}] calibração falhou: {exc}", file=sys.stderr)
            continue

        corr = 0.0 if math.isnan(result.corr) else result.corr
        if abs(corr) < abs(min_corr):
            print(f"[{a}/{b}] corr {corr:.3f} abaixo de {min_corr:.3f}")
            continue

        half_life = getattr(result, "half_life", 0.0)
        half_life = float(half_life) if not math.isnan(half_life) else 0.0

        out.append(PairScore(
            a=a,
            b=b,
            alpha=float(result.alpha) if result.alpha is not None else 0.0,
            beta=float(result.beta) if result.beta is not None else 1.0,
            corr=corr,
            half_life=half_life,
            score=abs(corr),
        ))
        kept += 1
        if idx % 200 == 0 or idx == total_pairs:
            print(f"progress: testados={idx}/{total_pairs} mantidos={kept} last_pair={a}/{b} corr={corr:.3f}")

    print(f"total testados={tested} mantidos={kept}")
    return out


def save_pairs(pairs: Iterable[PairScore], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["pair", "score", "corr", "alpha", "beta", "half_life"])
        for pair in pairs:
            writer.writerow([
                f"{pair.a}/{pair.b}",
                f"{pair.score:.12f}",
                f"{pair.corr:.12f}",
                f"{pair.alpha:.12f}",
                f"{pair.beta:.12f}",
                f"{pair.half_life:.12f}",
            ])


def main(
    min_corr: float = DEFAULT_MIN_CORR,
    pre_corr_buffer: float = DEFAULT_PRE_CORR_BUFFER,
    bars: int = DEFAULT_BARS,
    max_pairs: int = DEFAULT_MAX_PAIRS,
    out_path: str = "pairs_rank.csv",
    limit_universe: Optional[int] = None,
    shuffle_pairs: bool = False,
    mt5_cfg: Optional[MT5Config] = None,
) -> None:
    svc = MT5Service(mt5_cfg or MT5Config())
    if not svc.connect():
        print("Falha ao conectar no MT5.")
        return

    try:
        symbols = list(UNIVERSE[:limit_universe] if limit_universe else UNIVERSE)
        closes_cache: dict[str, list[float]] = {}
        for symbol in symbols:
            closes = collect_closes(svc, symbol, bars)
            if closes:
                closes_cache[symbol] = closes

        if not closes_cache:
            print("Nenhum símbolo com dados suficientes.")
            return

        returns_cache: dict[str, list[float]] = {}
        for symbol, closes in closes_cache.items():
            ret = compute_returns(closes)
            if len(ret) >= MIN_RETURNS:
                returns_cache[symbol] = ret
            else:
                print(f"[{symbol}] retornos curtos ({len(ret)}). Pulando.")

        if not returns_cache:
            print("Nenhum símbolo com retornos válidos.")
            return

        filtered = list(prefilter_pairs(returns_cache, min_corr, pre_corr_buffer))
        if not filtered:
            print("Nenhum par passou no pré-filtro.")
            return

        if shuffle_pairs:
            random.shuffle(filtered)
        pairs = filtered

        total_pairs = len(pairs)
        scored = calibrate_pairs(svc, pairs, bars, min_corr, total_pairs)
        if not scored:
            print("Nenhum par válido encontrado.")
            return

        scored.sort(key=lambda p: p.score, reverse=True)
        save_pairs(scored[:max_pairs], Path(out_path))
        print(f"{min(len(scored), max_pairs)} pares salvos em {out_path}")
    finally:
        svc.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gera pairs_rank.csv a partir do universo")
    parser.add_argument("--min-corr", type=float, default=DEFAULT_MIN_CORR,
                        help="correlação mínima absoluta necessária após calibração")
    parser.add_argument("--pre-corr-buffer", type=float, default=DEFAULT_PRE_CORR_BUFFER,
                        help="buffer aplicado ao pré-corr (min_corr - buffer)")
    parser.add_argument("--bars", type=int, default=DEFAULT_BARS,
                        help="quantas barras D1 carregar")
    parser.add_argument("--max-pairs", type=int, default=DEFAULT_MAX_PAIRS,
                        help="quantos pares salvos no CSV")
    parser.add_argument("--out", default="pairs_rank.csv",
                        help="arquivo de saída (diretório criado se necessário)")
    parser.add_argument("--limit-universe", type=int, default=None,
                        help="limita quantos símbolos do universo serão usados")
    parser.add_argument("--shuffle", action="store_true",
                        help="embaralha a ordem das combinações antes da calibração")
    parser.add_argument("--seed", type=int, default=None,
                        help="seed para o shuffle (opcional)")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    main(
        min_corr=args.min_corr,
        pre_corr_buffer=args.pre_corr_buffer,
        bars=args.bars,
        max_pairs=args.max_pairs,
        out_path=args.out,
        limit_universe=args.limit_universe,
        shuffle_pairs=args.shuffle,
    )

# Example:
# python services/build_pairs_from_universe.py --min-corr 0.8 --bars 250 --max-pairs 20 --out pairs_rank.csv
