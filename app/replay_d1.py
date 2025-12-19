from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

import MetaTrader5 as mt5

from services.mt5_connect import MT5Config, MT5Service
from services.pair_calibration import calibrate_pair_from_series
from services.multi_monitor import RollingZScore, SignalEngine


DEFAULT_PAIRS = "PETR4/PETR3,ITUB4/BBDC4,BBAS3/ITUB4"
DEFAULT_LOOKBACK = 300
DEFAULT_Z_WINDOW = 120
DEFAULT_WARMUP = 20
DEFAULT_ENTER_Z = 2.1
DEFAULT_EXIT_BAND = 0.2


@dataclass
class PairSpec:
    a: str
    b: str

    @property
    def name(self) -> str:
        return f"{self.a}/{self.b}"


@dataclass
class D1Bar:
    dt: datetime
    price_a: float
    price_b: float


class PairFeed:
    def __init__(self, mt5_service: MT5Service, spec: PairSpec, lookback: int):
        self._svc = mt5_service
        self.spec = spec
        self.lookback = lookback
        self.bars: list[D1Bar] = []

    def load(self) -> None:
        rates_a = self._svc.get_close_rates(self.spec.a, mt5.TIMEFRAME_D1, bars=self.lookback, shift=1)
        rates_b = self._svc.get_close_rates(self.spec.b, mt5.TIMEFRAME_D1, bars=self.lookback, shift=1)

        length = min(len(rates_a), len(rates_b))
        if length == 0:
            raise RuntimeError(f"Nenhum candle D1 disponível para {self.spec.name}")

        for idx in range(length):
            rate_a = rates_a[idx]
            rate_b = rates_b[idx]
            time_a = int(rate_a.get("time", 0))
            time_b = int(rate_b.get("time", 0))

            if time_a != time_b:
                # alinhamento ideal para pares B3, mas ignoramos ticks desalinhados
                continue

            dt = datetime.utcfromtimestamp(time_a)
            price_a = float(rate_a.get("close", 0.0) or 0.0)
            price_b = float(rate_b.get("close", 0.0) or 0.0)

            self.bars.append(D1Bar(dt=dt, price_a=price_a, price_b=price_b))

        if not self.bars:
            raise RuntimeError(f"Nenhum candle alinhado para {self.spec.name}")

    def closes_a(self) -> list[float]:
        return [bar.price_a for bar in self.bars]

    def closes_b(self) -> list[float]:
        return [bar.price_b for bar in self.bars]


class ReplayTradeLogger:
    HEADERS = [
        "exit_date", "pair", "side",
        "entry_z", "exit_z",
        "entry_spread", "exit_spread",
        "holding_days", "pnl_spread",
        "alpha", "beta", "corr",
    ]

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(self.path, "w", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file)
        self.writer.writerow(self.HEADERS)

    def log_trade(
        self,
        exit_date: datetime,
        pair: str,
        side: str,
        entry_z: float,
        exit_z: float,
        entry_spread: float,
        exit_spread: float,
        holding_days: float,
        pnl_spread: float,
        alpha: float,
        beta: float,
        corr: float,
    ) -> None:
        self.writer.writerow([
            exit_date.isoformat(),
            pair,
            side,
            f"{entry_z:+.3f}",
            f"{exit_z:+.3f}",
            f"{entry_spread:+.6f}",
            f"{exit_spread:+.6f}",
            f"{holding_days:.4f}",
            f"{pnl_spread:+.6f}",
            f"{alpha:.6f}",
            f"{beta:.6f}",
            f"{corr:.6f}",
        ])

    def close(self) -> None:
        self.file.close()

    def __enter__(self) -> "ReplayTradeLogger":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


@dataclass
class TradeState:
    entry_dt: datetime
    entry_spread: float
    entry_z: float
    side: str


class PairReplay:
    MIN_CALIBRATION_BARS = 50

    def __init__(
        self,
        spec: PairSpec,
        mt5_service: MT5Service,
        lookback: int,
        z_window: int,
        warmup: int,
        enter_z: float,
        exit_band: float,
        recalibrate: bool,
        min_corr: float,
    ):
        self.spec = spec
        self.lookback = lookback
        self.z_window = z_window
        self.warmup = warmup
        self.enter_z = enter_z
        self.exit_band = exit_band
        self.recalibrate = recalibrate
        self.min_corr = min_corr
        self.feed = PairFeed(mt5_service, spec, lookback)
        self.feed.load()
        self.z_score = RollingZScore(window=z_window, warmup=warmup)
        self.signal = SignalEngine(enter_z=enter_z, exit_band=exit_band)
        self.alpha: Optional[float] = None
        self.beta: Optional[float] = None
        self.corr: Optional[float] = None
        self.trade: Optional[TradeState] = None

    def _calibrate_up_to(self, count: int):
        if count < self.MIN_CALIBRATION_BARS:
            raise RuntimeError(f"precisa de >= {self.MIN_CALIBRATION_BARS} candles calibrados, tem {count}")

        bars = self.feed.bars[:count]
        ca = [bar.price_a for bar in bars]
        cb = [bar.price_b for bar in bars]
        return calibrate_pair_from_series(ca, cb)

    def initialize(self) -> bool:
        try:
            calib = self._calibrate_up_to(len(self.feed.bars))
        except Exception as exc:
            print(f"[{self.spec.name}] calibração inicial falhou: {exc}")
            return False

        if calib.corr < self.min_corr:
            print(f"[{self.spec.name}] correlação inicial baixa {calib.corr:.3f} < {self.min_corr:.2f}")
            return False

        self.alpha = calib.alpha
        self.beta = calib.beta
        self.corr = calib.corr
        print(f"[{self.spec.name}] calibrado | alpha={self.alpha:.6f} beta={self.beta:.6f} corr={self.corr:.3f}")
        return True

    def maybe_recalibrate(self, upto: int) -> bool:
        if not self.recalibrate:
            return True
        try:
            calib = self._calibrate_up_to(upto)
        except Exception:
            print(f"[{self.spec.name}] recalibração falhou (precisa de >= {self.MIN_CALIBRATION_BARS} candles)")
            return False

        if calib.corr < self.min_corr:
            return False

        self.alpha = calib.alpha
        self.beta = calib.beta
        self.corr = calib.corr
        return True

    def _pnl_spread(self, spread_exit: float, entry_spread: float, side: str) -> float:
        if side == "LONG_SPREAD":
            return spread_exit - entry_spread
        return entry_spread - spread_exit

    def run(self, logger: ReplayTradeLogger) -> None:
        for idx, bar in enumerate(self.feed.bars):
            if bar.price_a <= 0 or bar.price_b <= 0:
                continue

            if self.recalibrate:
                if not self.maybe_recalibrate(idx + 1):
                    continue
            elif self.alpha is None:
                continue

            if self.alpha is None or self.beta is None:
                continue

            spread = math.log(bar.price_a) - (self.alpha + self.beta * math.log(bar.price_b))
            z = self.z_score.update(spread)
            if z is None:
                continue

            status, side, _, _ = self.signal.on_tick(z, spread)

            if status == "ENTER" and self.trade is None:
                self.trade = TradeState(
                    entry_dt=bar.dt,
                    entry_spread=spread,
                    entry_z=z,
                    side=side,
                )
                continue

            if status == "EXIT" and self.trade:
                holding_secs = (bar.dt - self.trade.entry_dt).total_seconds()
                holding_days = holding_secs / 86400.0
                pnl = self._pnl_spread(spread, self.trade.entry_spread, self.trade.side)
                logger.log_trade(
                    exit_date=bar.dt,
                    pair=self.spec.name,
                    side=self.trade.side,
                    entry_z=self.trade.entry_z,
                    exit_z=z,
                    entry_spread=self.trade.entry_spread,
                    exit_spread=spread,
                    holding_days=holding_days,
                    pnl_spread=pnl,
                    alpha=self.alpha or 0.0,
                    beta=self.beta or 0.0,
                    corr=self.corr or 0.0,
                )
                self.trade = None


def parse_pairs(value: str) -> list[PairSpec]:
    out: list[PairSpec] = []
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "/" not in chunk:
            raise ValueError(f"par inválido: {chunk}")
        left, right = chunk.split("/", 1)
        out.append(PairSpec(left.strip().upper(), right.strip().upper()))
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay D1 usando dados MT5")
    parser.add_argument("--pairs", default=DEFAULT_PAIRS,
                        help="lista de pares separados por vírgula (ex: PETR4/PETR3,ITUB4/BBDC4)")
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK,
                        help="quantos candles D1 carregar")
    parser.add_argument("--z-window", type=int, default=DEFAULT_Z_WINDOW, dest="z_window",
                        help="janela do RollingZScore")
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP,
                        help="warmup do RollingZScore")
    parser.add_argument("--enter", type=float, default=DEFAULT_ENTER_Z,
                        help="limiar de entrada (|z|)")
    parser.add_argument("--exit", type=float, default=DEFAULT_EXIT_BAND,
                        help="limiar de saída (|z|)")
    parser.add_argument("--out", default="data/trades_replay.csv",
                        help="caminho do CSV de saídas")
    parser.add_argument("--recalibrate", action="store_true",
                        help="recalibrar OLS a cada candle")
    parser.add_argument("--min-corr", type=float, default=0.70,
                        help="corr mínima do OLS para processar o par")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        specs = parse_pairs(args.pairs)
    except ValueError as exc:
        print("Erro ao parsear pares:", exc)
        return

    if not specs:
        print("Nenhum par informado.")
        return

    cfg = MT5Config()
    svc = MT5Service(cfg)
    if not svc.connect():
        print("Falha ao conectar no MT5.")
        return

    try:
        with ReplayTradeLogger(args.out) as logger:
            for spec in specs:
                try:
                    pair = PairReplay(
                        spec=spec,
                        mt5_service=svc,
                        lookback=args.lookback,
                        z_window=args.z_window,
                        warmup=args.warmup,
                        enter_z=args.enter,
                        exit_band=args.exit,
                        recalibrate=args.recalibrate,
                        min_corr=args.min_corr,
                    )
                except Exception as exc:
                    print(f"[{spec.name}] falha ao carregar feed: {exc}")
                    continue

                if not pair.initialize():
                    continue

                pair.run(logger)
    finally:
        svc.disconnect()


if __name__ == "__main__":
    main()
