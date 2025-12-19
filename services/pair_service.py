from __future__ import annotations

import csv
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Deque, Optional, Literal

from services.hedge import estimate_ols_from_mt5, HedgeOLS

import MetaTrader5 as mt5


PriceMode = Literal["last", "mid"]


@dataclass
class TickPrice:
    symbol: str
    bid: float
    ask: float
    last: float
    time: int

    def price(self, mode: PriceMode = "last") -> float:
        if mode == "mid":
            if self.bid and self.ask:
                return (self.bid + self.ask) / 2.0
        return self.last


class MT5Client:
    """Cliente simples (baixo nível) para buscar ticks no MT5."""
    def __init__(self):
        self.connected = False

    def connect(self) -> bool:
        if not mt5.initialize():
            print("❌ Falha ao conectar no MT5:", mt5.last_error())
            return False
        self.connected = True
        return True

    def shutdown(self) -> None:
        mt5.shutdown()
        self.connected = False

    def tick(self, symbol: str) -> Optional[TickPrice]:
        if not self.connected:
            return None

        t = mt5.symbol_info_tick(symbol)
        if t is None:
            return None

        return TickPrice(
            symbol=symbol,
            bid=float(t.bid),
            ask=float(t.ask),
            last=float(t.last),
            time=int(t.time),
        )


class RollingZScore:
    """Z-score em janela móvel."""
    def __init__(self, window: int = 120):
        self.window = window
        self.values: Deque[float] = deque(maxlen=window)

    def update(self, x: float) -> Optional[float]:
        self.values.append(x)
        if len(self.values) < max(20, self.window // 5):  # aquecimento mínimo
            return None

        mean = sum(self.values) / len(self.values)
        var = sum((v - mean) ** 2 for v in self.values) / len(self.values)
        std = math.sqrt(var)

        if std == 0:
            return 0.0

        return (x - mean) / std


@dataclass
class Position:
    side: str               # "LONG_SPREAD" ou "SHORT_SPREAD"
    entry_z: float
    entry_spread: float


class SignalEngine:
    """
    Regras:
    - ENTER:
        z >= enter_z  -> SHORT_SPREAD (short A / long B)
        z <= -enter_z -> LONG_SPREAD  (long A / short B)
    - EXIT:
        posição aberta fecha quando z cruza 0 na direção do mean-reversion
        LONG_SPREAD: fecha quando z >= 0
        SHORT_SPREAD: fecha quando z <= 0
    """
    def __init__(self, enter_z: float = 2.1):
        self.enter_z = enter_z
        self.pos: Position | None = None

    def on_tick(self, z: float, spread: float) -> tuple[str, str]:
        """
        Retorna (status, event)
        status: HOLD / IN_POSITION / ENTER / EXIT
        event: texto detalhado
        """
        if self.pos is None:
            if z >= self.enter_z:
                self.pos = Position("SHORT_SPREAD", entry_z=z, entry_spread=spread)
                return "ENTER", "SHORT A / LONG B"
            if z <= -self.enter_z:
                self.pos = Position("LONG_SPREAD", entry_z=z, entry_spread=spread)
                return "ENTER", "LONG A / SHORT B"
            return "HOLD", "HOLD"

        if self.pos.side == "LONG_SPREAD":
            if z >= 0:
                pnl_spread = spread - self.pos.entry_spread
                self.pos = None
                return "EXIT", f"LONG_SPREAD pnl(spread)={pnl_spread:+.6f}"
            return "IN_POSITION", "LONG_SPREAD"

        if self.pos.side == "SHORT_SPREAD":
            if z <= 0:
                pnl_spread = self.pos.entry_spread - spread
                self.pos = None
                return "EXIT", f"SHORT_SPREAD pnl(spread)={pnl_spread:+.6f}"
            return "IN_POSITION", "SHORT_SPREAD"

        return "HOLD", "HOLD"


class TradeLogger:
    def __init__(self, filepath: str = "trades.csv"):
        self.path = Path(filepath)
        self._init_file()

    def _init_file(self):
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    "ts", "pair", "event", "side",
                    "z", "spread",
                    "alpha", "beta",
                    "pnl_spread"
                ])

    def log(self, pair: str, event: str, side: str, z: float, spread: float,
            alpha: float, beta: float, pnl_spread: str = ""):
        with self.path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                datetime.now().isoformat(timespec="seconds"),
                pair,
                event,
                side,
                f"{z:+.4f}",
                f"{spread:+.6f}",
                f"{alpha:.6f}",
                f"{beta:.6f}",
                pnl_spread
            ])



class PairService:
    def __init__(
        self,
        sym_a: str,
        sym_b: str,
        hedge: float = 1.0,
        price_mode: PriceMode = "last",
        z_window: int = 120,
    ):
        self.sym_a = sym_a
        self.sym_b = sym_b
        self.alpha = 0.0
        self.hedge = hedge
        self.price_mode = price_mode
        self.z = RollingZScore(window=z_window)
        self.mt5 = MT5Client()
        self.engine = SignalEngine(enter_z=2.1)
        self.logger = TradeLogger("trades.csv")

    def connect(self) -> bool:
        ok = self.mt5.connect()
        if ok:
            print(f"✅ MT5 conectado. Par: {self.sym_a}/{self.sym_b} hedge={self.hedge} mode={self.price_mode}")
        return ok

    def shutdown(self) -> None:
        self.mt5.shutdown()

    def step(self) -> None:
        ta = self.mt5.tick(self.sym_a)
        tb = self.mt5.tick(self.sym_b)

        if not ta or not tb:
            print("⚠️ Sem tick de um dos ativos (talvez símbolo inválido/mercado fechado).")
            return

        pa = ta.price(self.price_mode)
        pb = tb.price(self.price_mode)

        # Consistência com OLS em log:
        pa = math.log(pa) if pa > 0 else pa
        pb = math.log(pb) if pb > 0 else pb

        spread = pa - (self.alpha + self.hedge * pb)
        z = self.z.update(spread)

        if z is None:
            print(f"📈 Spread={spread:.5f} (aquecendo z-score...) A={pa:.2f} B={pb:.2f}")
            return

        status, event = self.engine.on_tick(z, spread)

        pair_name = f"{self.sym_a}/{self.sym_b}"

        if status in ("ENTER", "EXIT"):
            side = self.engine.pos.side if (self.engine.pos is not None and status != "EXIT") else ""
            inferred_side = (
                "LONG_SPREAD"
                if "LONG_SPREAD" in event
                else ("SHORT_SPREAD" if "SHORT_SPREAD" in event else side)
            )

            pnl_str = ""
            if "pnl(spread)=" in event:
                pnl_str = event.split("pnl(spread)=")[-1].strip()

            self.logger.log(
                pair=pair_name,
                event=status,
                side=inferred_side,
                z=z,
                spread=spread,
                alpha=self.alpha,
                beta=self.hedge,
                pnl_spread=pnl_str,
            )

        print(
            f"📊 A={pa:.4f} B={pb:.4f} | spread={spread:+.5f} | z={z:+.2f} | {status}: {event}"
        )


if __name__ == "__main__":
    SYM_A = "ITUB4"
    SYM_B = "BBDC4"

    ols: HedgeOLS | None = estimate_ols_from_mt5(SYM_A, SYM_B, timeframe=mt5.TIMEFRAME_D1, bars=400, mode="log")
    if ols:
        print(f"📌 OLS: alpha={ols.alpha:.6f} beta={ols.beta:.6f} corr={ols.corr:.3f} n={ols.n}")
        hedge = ols.beta
        alpha = ols.alpha
    else:
        print("⚠️ Não consegui estimar OLS, usando fallback alpha=0 beta=1")
        hedge = 1.0
        alpha = 0.0

    svc = PairService(SYM_A, SYM_B, hedge=hedge, price_mode="last", z_window=120)
    svc.alpha = alpha  # injeta alpha

    if svc.connect():
        try:
            while True:
                svc.step()
                sleep(2)
        except KeyboardInterrupt:
            pass
        finally:
            svc.shutdown()
            print("👋 Finalizado.")
