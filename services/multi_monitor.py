from __future__ import annotations

import csv
import json
import math
import os
import time
from dataclasses import dataclass
from collections import deque
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Deque, Optional

import MetaTrader5 as mt5

from services.mt5_connect import IPC_FAIL, MT5Config, MT5Service
from services.pair_calibration import calibrate_pair_from_mt5


# -----------------------------
# Helpers
# -----------------------------
def symbol_exists(mt5_service: MT5Service, sym: str) -> bool:
    try:
        mt5_service.ensure_symbol(sym)
        return True
    except Exception:
        return False
# -----------------------------
# Utils
# -----------------------------
def now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def write_monitor_state(path: Path, payload: dict[str, Any]) -> None:
    """
    Atomically dump the monitor payload so Django can read a full snapshot.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f"{path.name}.tmp"
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp_path.replace(path)


HISTORY_MAXLEN = 600
_PAIR_HISTORY: dict[str, Deque[dict[str, Any]]] = {}


def write_monitor_history(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f"{path.name}.tmp"
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp_path.replace(path)


def append_history_entry(pair: str, entry: dict[str, Any]) -> None:
    buf = _PAIR_HISTORY.get(pair)
    if buf is None:
        buf = deque(maxlen=HISTORY_MAXLEN)
        _PAIR_HISTORY[pair] = buf
    buf.append(entry)


EPS_STD = 1e-9
DEFAULT_STALE_SECONDS = 300.0


_LAST_TICK_MSC: dict[str, int] = {}
_LAST_SEEN_LOCAL: dict[str, float] = {}


class EntryConfirmMode(Enum):
    NONE = "none"
    CONSECUTIVE = "consecutive"
    INCREASING = "increasing"


def get_tick_state(symbol: str, stale_seconds: float = DEFAULT_STALE_SECONDS) -> tuple[Optional[float], Optional[float], bool, str]:
    """
    Return (price, age, same_tick, status).
      - price: float | None
      - age: segundos desde o último tick observado localmente
      - same_tick: bool (baseado apenas em time_msc)
      - status: "OK" | "STALE" | "NO_TICK"
    """
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None, None, False, "NO_TICK"

    last = float(getattr(tick, "last", 0.0) or 0.0)
    bid = float(getattr(tick, "bid", 0.0) or 0.0)
    ask = float(getattr(tick, "ask", 0.0) or 0.0)

    if last > 0:
        price = last
    elif bid > 0 and ask > 0:
        price = (bid + ask) / 2.0
    else:
        price = bid or ask or None

    msc = int(getattr(tick, "time_msc", 0) or 0)
    now = time.time()

    prev_msc = _LAST_TICK_MSC.get(symbol)
    same_tick = (prev_msc == msc) if (prev_msc is not None and msc > 0) else False

    if prev_msc is None or msc != prev_msc:
        _LAST_TICK_MSC[symbol] = msc
        _LAST_SEEN_LOCAL[symbol] = now

    last_seen = _LAST_SEEN_LOCAL.get(symbol, now)
    age = now - last_seen

    status = "STALE" if age > stale_seconds else "OK"
    return price, age, same_tick, status


def parse_entry_confirm_mode(value: str) -> EntryConfirmMode:
    normalized = value.lower().strip()
    for mode in EntryConfirmMode:
        if mode.value == normalized:
            return mode
    raise ValueError(f"confirm mode inválido: {value}")


def save_pairs_cache(pairs: list["PairCfg"], path: str = "data/pairs_last_good.csv") -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["pair", "score", "corr", "alpha", "beta", "half_life"])
        for p in pairs:
            w.writerow([
                f"{p.a}/{p.b}",
                f"{p.score:.12f}",
                f"{p.corr:.12f}",
                f"{p.alpha:.12f}",
                f"{p.beta:.12f}",
                f"{p.half_life:.12f}",
            ])


# -----------------------------
# Rolling Z-Score
# -----------------------------
class RollingZScore:
    def __init__(self, window: int = 120, warmup: int = 30):
        self.window = window
        self.warmup = warmup
        self.values: Deque[float] = deque(maxlen=window)
        self.last_mean: float | None = None
        self.last_std: float | None = None

    def update(self, x: float) -> Optional[float]:
        self.values.append(x)
        if len(self.values) < min(self.warmup, self.window):
            self.last_mean = None
            self.last_std = None
            return None

        mean = sum(self.values) / len(self.values)
        var = sum((v - mean) ** 2 for v in self.values) / len(self.values)
        std = math.sqrt(var) if var > 0 else 0.0
        self.last_mean = mean
        self.last_std = std
        if std < EPS_STD:
            return None
        return (x - mean) / std


# -----------------------------
# Signal Engine (paper)
# -----------------------------
@dataclass
class Position:
    side: str  # "LONG_SPREAD" or "SHORT_SPREAD"
    entry_z: float
    entry_spread: float
    entry_ts: str


class SignalEngine:
    def __init__(self, enter_z: float = 2.1, exit_band: float = 0.2):
        self.enter_z = enter_z
        self.exit_band = exit_band
        self.pos: Position | None = None

    def on_tick(self, z: float, spread: float) -> tuple[str, str, str, str]:
        """
        returns (status, side, details, pnl_spread_str)
        status: HOLD / ENTER / IN_POSITION / EXIT
        side: LONG_SPREAD / SHORT_SPREAD / ""
        pnl_spread_str: string or ""
        """
        if self.pos is None:
            if z >= self.enter_z:
                self.pos = Position("SHORT_SPREAD", z, spread, now_ts())
                return "ENTER", "SHORT_SPREAD", "SHORT A / LONG B", ""
            if z <= -self.enter_z:
                self.pos = Position("LONG_SPREAD", z, spread, now_ts())
                return "ENTER", "LONG_SPREAD", "LONG A / SHORT B", ""
            return "HOLD", "", "HOLD", ""

        # exit when back near 0 (band)
        if abs(z) <= self.exit_band:
            if self.pos.side == "LONG_SPREAD":
                pnl = spread - self.pos.entry_spread
            else:  # SHORT_SPREAD
                pnl = self.pos.entry_spread - spread

            side = self.pos.side
            self.pos = None
            return "EXIT", side, f"exit_band | pnl(spread)={pnl:+.6f}", f"{pnl:+.6f}"

        return "IN_POSITION", self.pos.side, self.pos.side, ""


# -----------------------------
# Logger
# -----------------------------
class MultiTradeLogger:
    def __init__(self, filepath: str = "trades_multi.csv"):
        self.path = Path(filepath)
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    "ts", "pair", "event", "side",
                    "z", "spread", "alpha", "beta",
                    "pnl_spread"
                ])

    def log(self, pair: str, event: str, side: str, z: float, spread: float,
            alpha: float, beta: float, pnl_spread: str = ""):
        with self.path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                now_ts(),
                pair,
                event,
                side,
                f"{z:+.4f}",
                f"{spread:+.6f}",
                f"{alpha:.6f}",
                f"{beta:.6f}",
                pnl_spread
            ])


# -----------------------------
# Pair Runtime
# -----------------------------
@dataclass
class PairCfg:
    a: str
    b: str
    alpha: float
    beta: float
    corr: float
    half_life: float
    score: float


class PairRuntime:
    def __init__(
        self,
        cfg: PairCfg,
        z_window: int,
        warmup: int,
        enter_z: float,
        exit_band: float,
        mt5_service: MT5Service,
        stale_seconds: float,
        entry_confirm_mode: EntryConfirmMode,
        sigma_min: float,
    ):
        self.cfg = cfg
        self.z = RollingZScore(window=z_window, warmup=warmup)
        self.engine = SignalEngine(enter_z=enter_z, exit_band=exit_band)
        self.mt5_service = mt5_service
        self.stale_seconds = stale_seconds
        self.confirm_mode = entry_confirm_mode
        self.sigma_min = sigma_min
        self.confirm_count = 0
        self.prev_abs_z: Optional[float] = None
        self._was_idle: bool = False
        self._last_spread: Optional[float] = None
        self._wake_line: Optional[str] = None
        self.last_state: dict[str, Any] | None = None

    def _reset_entry_state(self) -> None:
        self.confirm_count = 0
        self.prev_abs_z = None

    def _entry_block_reason(self, z: float) -> Optional[str]:
        abs_z = abs(z)
        if self.sigma_min > 0:
            std = self.z.last_std or 0.0
            if std < self.sigma_min:
                return f"hold(sigma={std:.6f}<{self.sigma_min})"

        if self.confirm_mode == EntryConfirmMode.NONE:
            return None

        if self.confirm_mode == EntryConfirmMode.CONSECUTIVE:
            if abs_z >= self.engine.enter_z:
                self.confirm_count += 1
            else:
                self.confirm_count = 0
            if self.confirm_count >= 2:
                return None
            return f"hold(confirm {self.confirm_count}/2 | |z|={abs_z:.3f})"

        if self.confirm_mode == EntryConfirmMode.INCREASING:
            prev = self.prev_abs_z
            allowed = prev is not None and abs_z >= self.engine.enter_z and abs_z > prev
            self.prev_abs_z = abs_z
            if allowed:
                return None
            prev_str = f"{prev:.3f}" if prev is not None else "n/a"
            return f"hold(increasing prev={prev_str} cur={abs_z:.3f})"

        return None

    def pop_wake_line(self) -> Optional[str]:
        line = self._wake_line
        self._wake_line = None
        return line

    def step(self):
        pair_name = f"{self.cfg.a}/{self.cfg.b}"
        self._wake_line = None
        pair_ts = now_ts()
        state: dict[str, Any] = {
            "pair": pair_name,
            "a": self.cfg.a,
            "b": self.cfg.b,
            "alpha": self.cfg.alpha,
            "beta": self.cfg.beta,
            "corr": self.cfg.corr,
            "half_life": self.cfg.half_life,
            "last_update_ts": pair_ts,
            "status": "",
            "phase": "ready",
            "warm": None,
            "wake": False,
            "sameA": False,
            "sameB": False,
            "pa": None,
            "pb": None,
            "spread": None,
            "z": None,
            "signal": "",
            "side": "",
            "details": "",
        }

        try:
            pa, age_a, same_a, st_a = get_tick_state(self.cfg.a, self.stale_seconds)
            pb, age_b, same_b, st_b = get_tick_state(self.cfg.b, self.stale_seconds)
        except Exception as e:
            msg = f"MT5_ERR: {e}"
            state.update({
                "status": "mt5_err",
                "signal": "MT5_ERR",
                "details": msg,
            })
            self.last_state = state
            return pair_name, msg

        state.update({
            "pa": pa,
            "pb": pb,
            "sameA": same_a,
            "sameB": same_b,
        })

        if pa is None or pb is None:
            fail = self.mt5_service.last_fail()
            status = "NO_TICK"
            if fail and fail[0] == IPC_FAIL:
                status = "MT5_DOWN"
                time.sleep(2.0)

            msg = f"{status} | a={pa} b={pb} | stA={st_a} stB={st_b}"
            if fail:
                msg = f"{msg} | fail={fail}"
            state.update({
                "status": status.lower(),
                "signal": status,
                "details": msg,
            })
            self.last_state = state
            return pair_name, msg

        pair_stale = (st_a == "STALE") or (st_b == "STALE")
        la = math.log(pa)
        lb = math.log(pb)
        spread = la - (self.cfg.alpha + self.cfg.beta * lb)
        is_idle = same_a and same_b

        if not pair_stale:
            woke_up = self._was_idle and not is_idle
            spread_changed = self._last_spread is None or abs(spread - self._last_spread) > 1e-9
            if woke_up and spread_changed:
                # Only trigger WAKE when leaving idle with a real spread movement.
                self._wake_line = f"WAKE {pair_name} | pa={pa:.5f} pb={pb:.5f} spread={spread:+.12f}"
        self._was_idle = is_idle
        self._last_spread = spread

        state.update({
            "spread": spread,
            "wake": bool(self._wake_line),
        })

        if pair_stale:
            def fmt_age(value: Optional[float]) -> str:
                return f"{value:.1f}s" if value is not None else "n/a"

            status = "STALE"
            age_a_str = fmt_age(age_a)
            age_b_str = fmt_age(age_b)
            msg = (
                f"{status} | pa={pa:.5f} pb={pb:.5f} "
                f"| ageA={age_a_str} ageB={age_b_str} "
                f"| sameA={same_a} sameB={same_b} | stA={st_a} stB={st_b}"
            )
            state.update({
                "status": "stale",
                "signal": "STALE",
                "details": msg,
            })
            self.last_state = state
            return pair_name, msg

        if is_idle:
            msg = (
                f"idle | pa={pa:.5f} pb={pb:.5f} "
                f"| sameA={same_a} sameB={same_b} spread={spread:+.12f}"
            )
            state.update({
                "status": "idle",
                "signal": "QUIET",
                "details": msg,
            })
            self.last_state = state
            return pair_name, msg

        z = self.z.update(spread)
        if z is None:
            if self.z.last_std is not None and self.z.last_std < EPS_STD:
                msg = f"flat(std~0) pa={pa:.5f} pb={pb:.5f} spread={spread:+.12f}"
            else:
                target = min(self.z.warmup, self.z.window)
                count = len(self.z.values)
                msg = f"active(warming {count}/{target})"
                if self.z.last_std is not None:
                    msg += f" std={self.z.last_std:.6f}"
                msg = f"{msg} | pa={pa:.5f} pb={pb:.5f} spread={spread:+.12f}"
            state.update({
                "status": "active",
                "phase": "warming",
                "warm": f"{len(self.z.values)}/{min(self.z.warmup, self.z.window)}",
                "signal": "WARMING",
                "details": msg,
            })
            self.last_state = state
            return pair_name, msg

        state["z"] = z
        in_position = self.engine.pos is not None
        if not in_position:
            block = self._entry_block_reason(z)
            if block:
                msg = (
                    f"{block} | pa={pa:.5f} pb={pb:.5f} "
                    f"| z={z:+.3f} spread={spread:+.12f}"
                )
                state.update({
                    "status": "active",
                    "signal": "HOLD",
                    "details": msg,
                })
                self.last_state = state
                return pair_name, msg

        status, side, details, pnl_str = self.engine.on_tick(z, spread)

        if status in ("ENTER", "EXIT"):
            self._reset_entry_state()

        msg = f"active | z={z:+.2f} pa={pa:.5f} pb={pb:.5f} spread={spread:+.12f} {status} {side} {details}".strip()
        state.update({
            "status": "active",
            "signal": status,
            "side": side,
            "details": details or msg,
        })
        self.last_state = state
        return pair_name, msg, status, side, z, spread, pnl_str


# -----------------------------
# CSV loader
# -----------------------------
def load_top_pairs(mt5_service: MT5Service, csv_path: str = "pairs_rank.csv", top_n: int = 5) -> list[PairCfg]:
    def _try_load(path: str) -> list[PairCfg]:
        if not Path(path).exists():
            return []
        out: list[PairCfg] = []
        with open(path, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                if len(out) >= top_n:
                    break
                pair = (row.get("pair") or "").strip()
                if "/" not in pair:
                    continue
                a, b = pair.split("/")
                out.append(PairCfg(
                    a=a, b=b,
                    score=float(row.get("score") or 0.0),
                    corr=float(row.get("corr") or 0.0),
                    alpha=float(row.get("alpha") or 0.0),
                    beta=float(row.get("beta") or 1.0),
                    half_life=float(row.get("half_life") or 0.0),
                ))
        return out

    # 1) tenta o CSV atual
    pairs = _try_load(csv_path)
    if pairs:
        save_pairs_cache(pairs, "data/pairs_last_good.csv")
        print("Cache salvo: data/pairs_last_good.csv (from pairs_rank.csv)")
        return pairs

    # 2) tenta o último bom
    pairs = _try_load("data/pairs_last_good.csv")
    if pairs:
        print("Usando cache: data/pairs_last_good.csv")
        return pairs

    # 3) fallback hardcoded (para mercado fechado / primeira execução)
    print("Nenhum CSV válido. Usando pares default (com calibração OLS).")

    defaults_raw = [
        ("PETR4", "PETR3"),
        ("ITUB4", "BBDC4"),
        ("ELET3", "ELET6"),
        ("BBAS3", "ITUB4"),
    ]

    calibrated: list[PairCfg] = []
    for a, b in defaults_raw:
        if not symbol_exists(mt5_service, a) or not symbol_exists(mt5_service, b):
            print(f"Ignorado {a}/{b}: símbolo indisponível")
            continue
        try:
            cal = calibrate_pair_from_mt5(mt5_service, a, b, bars=200)
            if cal.corr < 0.70:
                print(f"Ignorado {a}/{b}: corr baixa {cal.corr:.3f}")
                continue
            calibrated.append(PairCfg(a, b, alpha=cal.alpha, beta=cal.beta, corr=cal.corr, half_life=0.0, score=0.0))
            print(f"Calibrado {a}/{b}: alpha={cal.alpha:.4f} beta={cal.beta:.4f} corr={cal.corr:.3f} n={cal.n}")
        except Exception as e:
            print(f"Falha calibrando {a}/{b}: {e}")

    if calibrated:
        top = calibrated[:top_n]
        save_pairs_cache(top, "data/pairs_last_good.csv")
        print("Cache salvo: data/pairs_last_good.csv (from calibration)")
        return top

    print("Nenhum par calibrado. Usando pares default sem calibração.")
    defaults = [
        PairCfg("PETR4", "PETR3", alpha=0.0, beta=1.0, corr=0.0, half_life=0.0, score=0.0),
        PairCfg("ITUB4", "BBDC4", alpha=0.0, beta=1.0, corr=0.0, half_life=0.0, score=0.0),
        PairCfg("ELET3", "ELET6", alpha=0.0, beta=1.0, corr=0.0, half_life=0.0, score=0.0),
        PairCfg("BBAS3", "ITUB4", alpha=0.0, beta=1.0, corr=0.0, half_life=0.0, score=0.0),
    ]
    return defaults[:top_n]


# -----------------------------
# Main
# -----------------------------
def main(
    top_n: int = 5,
    enter_z: float = 2.3,
    exit_band: float = 0.3,
    z_window: int = 240,
    warmup: int = 60,
    poll_seconds: float = 5.0,
    stale_seconds: float = DEFAULT_STALE_SECONDS,
    entry_confirm_mode: str = EntryConfirmMode.NONE.value,
    sigma_min: float = 0.0,
    mt5_config: Optional[MT5Config] = None,
):
    try:
        confirm_mode = parse_entry_confirm_mode(entry_confirm_mode)
    except ValueError as exc:
        print("Entrada confirm mode inválido:", exc, "usando none")
        confirm_mode = EntryConfirmMode.NONE

    print(
        "MULTI_MONITOR FILE =", os.path.abspath(__file__),
        "params:", top_n, enter_z, exit_band, z_window, warmup, poll_seconds, stale_seconds,
        "confirm_mode=", confirm_mode.value, "sigma_min=", sigma_min
    )
    cfg = mt5_config or MT5Config()
    mt5_service = MT5Service(cfg)
    if not mt5_service.connect():
        print("Falha ao conectar no MT5.")
        return

    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    logger = MultiTradeLogger("data/trades_multi.csv")
    monitor_state_path = data_dir / "monitor_state.json"
    monitor_history_path = data_dir / "monitor_history.json"
    monitor_state_params = {
        "top_n": top_n,
        "enter_z": enter_z,
        "exit_band": exit_band,
        "z_window": z_window,
        "warmup": warmup,
        "poll_seconds": poll_seconds,
        "stale_seconds": stale_seconds,
        "entry_confirm_mode": confirm_mode.value,
        "sigma_min": sigma_min,
    }

    try:
        pairs = load_top_pairs(mt5_service, "pairs_rank.csv", top_n=top_n)
        if not pairs:
            print("Nenhum par carregado do pairs_rank.csv")
            return

        runtimes = [
            PairRuntime(
                cfg,
                z_window=z_window,
                warmup=warmup,
                enter_z=enter_z,
                exit_band=exit_band,
                mt5_service=mt5_service,
                stale_seconds=stale_seconds,
                entry_confirm_mode=confirm_mode,
                sigma_min=sigma_min,
            )
            for cfg in pairs
        ]

        print(f"Multi-monitor iniciado | top_n={top_n} enter_z={enter_z} exit_band={exit_band} z_window={z_window} poll={poll_seconds}s")
        print("Pares:", ", ".join([f"{p.a}/{p.b}" for p in pairs]))

        while True:
            lines = []
            wake_lines = []
            for rt in runtimes:
                res = rt.step()
                wake_line = rt.pop_wake_line()
                if wake_line:
                    wake_lines.append(wake_line)

                state = rt.last_state
                if state:
                    append_history_entry(state["pair"], {
                        "ts": state["last_update_ts"],
                        "z": state.get("z"),
                        "spread": state.get("spread"),
                        "pa": state.get("pa"),
                        "pb": state.get("pb"),
                        "status": state.get("status"),
                        "signal": state.get("signal"),
                        "side": state.get("side"),
                        "wake": state.get("wake"),
                    })

                # backward-compatible unpack:
                if len(res) == 2:
                    pair_name, msg = res
                    lines.append(f"{pair_name:<12} | {msg}")
                    continue

                pair_name, msg, status, side, z, spread, pnl_str = res
                lines.append(f"{pair_name:<12} | {msg}")

                if status in ("ENTER", "EXIT"):
                    logger.log(
                        pair=pair_name,
                        event=status,
                        side=side,
                        z=z,
                        spread=spread,
                        alpha=rt.cfg.alpha,
                        beta=rt.cfg.beta,
                        pnl_spread=pnl_str,
                    )

                    # alerta visual forte
                    print("\n" + ("=" * 70))
                    print(f"ALERTA {status} {pair_name} | {msg}")
                    print(("=" * 70) + "\n")

            for wl in wake_lines:
                print("  " + wl)
            cycle_ts = now_ts()
            print(cycle_ts)
            for ln in lines:
                print("  " + ln)

            payload = {
                "ts": cycle_ts,
                "params": monitor_state_params,
                "pairs": [rt.last_state for rt in runtimes if rt.last_state],
            }
            write_monitor_state(monitor_state_path, payload)
            history_payload = {
                "ts": cycle_ts,
                "params": monitor_state_params,
                "history": {pair: list(buf) for pair, buf in _PAIR_HISTORY.items()},
            }
            write_monitor_history(monitor_history_path, history_payload)

            print("-" * 70)
            time.sleep(poll_seconds)

    except KeyboardInterrupt:
        print("\nFinalizado (Ctrl+C).")
    finally:
        mt5_service.disconnect()


if __name__ == "__main__":
    # Ajuste aqui se quiser:
    # Warmup=60 with poll_seconds=5 means the rolling score needs ~5 minutes to fill;
    # for faster iteration use z_window=120, warmup=20, poll_seconds=1-2.
    main(
        top_n=5,
        enter_z=2.3,
        exit_band=0.3,
        z_window=240,
        warmup=60,
        poll_seconds=5.0,
        stale_seconds=DEFAULT_STALE_SECONDS,
    )
