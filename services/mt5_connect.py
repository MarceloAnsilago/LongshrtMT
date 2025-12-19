# services/mt5_connect.py
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple

import MetaTrader5 as mt5


IPC_FAIL = -10001


@dataclass
class MT5Config:
    # Se você tiver 2 MT5 instalados, preencha 'path' com o terminal correto.
    path: Optional[str] = None          # ex: r"C:\Program Files\MetaTrader 5\terminal64.exe"
    portable: bool = False              # True se for instalação portable
    login: Optional[int] = None
    password: Optional[str] = None
    server: Optional[str] = None

    # Robustez
    init_retries: int = 8
    backoff_seconds: float = 1.0        # será multiplicado progressivamente (1,2,3,5,8...)
    max_backoff: float = 15.0


class MT5Service:
    def __init__(self, cfg: Optional[MT5Config] = None) -> None:
        self.cfg = cfg or MT5Config()
        self._connected = False
        self._last_init_ts: float = 0.0
        self._fail_count: int = 0
        self._last_fail: Optional[Tuple[int, str]] = None
        self._last_tick_msc: dict[str, int] = {}
        self._last_tick_epoch: dict[str, float] = {}

    # ----------------------------
    # conexão / reconexão
    # ----------------------------
    def connect(self) -> bool:
        """Conecta com retries/backoff e valida terminal+conta."""
        # evita ficar batendo init em loop apertado
        now = time.time()
        if self._connected and (now - self._last_init_ts) < 1.0:
            return True

        self.shutdown()

        delay = self.cfg.backoff_seconds
        for attempt in range(1, self.cfg.init_retries + 1):
            ok = self._initialize_once()
            if ok:
                self._connected = True
                self._fail_count = 0
                self._last_fail = None
                self._last_init_ts = time.time()
                return True

            code, msg = mt5.last_error()
            self._last_fail = (code, msg)
            self._connected = False
            self._fail_count += 1

            # backoff progressivo
            time.sleep(min(delay, self.cfg.max_backoff))
            delay = min(delay * 1.6, self.cfg.max_backoff)

        return False

    def _initialize_once(self) -> bool:
        kwargs = {}
        if self.cfg.path:
            kwargs["path"] = self.cfg.path
        if self.cfg.portable:
            kwargs["portable"] = True

        # se você quiser forçar login/server (opcional)
        if self.cfg.login and self.cfg.password and self.cfg.server:
            kwargs["login"] = self.cfg.login
            kwargs["password"] = self.cfg.password
            kwargs["server"] = self.cfg.server

        ok = mt5.initialize(**kwargs)
        if not ok:
            return False

        # sanity checks
        ti = mt5.terminal_info()
        ai = mt5.account_info()
        if ti is None or ai is None:
            # pode acontecer durante "restoring history"
            mt5.shutdown()
            return False

        return True

    def shutdown(self) -> None:
        try:
            mt5.shutdown()
        except Exception:
            pass
        self._connected = False

    def disconnect(self) -> None:
        self.shutdown()

    def is_connected(self) -> bool:
        return self._connected

    def last_fail(self) -> Optional[Tuple[int, str]]:
        return self._last_fail

    def record_last_error(self, code: int, msg: str) -> None:
        self._record_fail(code, msg)

    def ensure_connected(self) -> None:
        if not self._connected:
            raise RuntimeError("MT5Service não está conectado. Chame connect().")

    def _record_fail(self, code: int, msg: str) -> None:
        self._last_fail = (code, msg)

    # ----------------------------
    # helpers de símbolo / preço
    # ----------------------------
    def ensure_symbol(self, symbol: str) -> bool:
        """Ensure the symbol is visible in Market Watch."""
        if not self._connected and not self.connect():
            return False

        info = mt5.symbol_info(symbol)
        if info is None:
            code, msg = mt5.last_error()
            self._record_fail(code, msg)
            if code == IPC_FAIL:
                self._connected = False
                if not self.connect():
                    return False
                info = mt5.symbol_info(symbol)
                if info is None:
                    return False
            else:
                return False

        if not info.visible:
            if not mt5.symbol_select(symbol, True):
                code, msg = mt5.last_error()
                self._record_fail(code, msg)
                if code == IPC_FAIL:
                    self._connected = False
                return False

        return True

    def get_last_price(self, symbol: str) -> Optional[float]:
        """Retorna last (preferência), senão mid(bid/ask)."""
        if not self.ensure_symbol(symbol):
            return None

        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            code, msg = mt5.last_error()
            self._record_fail(code, msg)
            if code == IPC_FAIL:
                self._connected = False
            return None

        # last pode vir 0 em alguns ativos/condições -> fallback mid
        last = float(getattr(tick, "last", 0.0) or 0.0)
        bid = float(getattr(tick, "bid", 0.0) or 0.0)
        ask = float(getattr(tick, "ask", 0.0) or 0.0)

        if last > 0:
            return last
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        if bid > 0:
            return bid
        if ask > 0:
            return ask
        return None

    def get_last_price_with_meta(self, symbol: str) -> tuple[Optional[float], dict]:
        """Return the price and metadata used by the monitor."""
        if not self.ensure_symbol(symbol):
            return None, {"err": f"symbol ensure failed: {symbol}"}

        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            code, msg = mt5.last_error()
            self._record_fail(code, msg)
            if code == IPC_FAIL:
                self._connected = False
            return None, {"err": f"tick None: {symbol} | {code} {msg}"}

        last = float(getattr(tick, "last", 0.0) or 0.0)
        bid = float(getattr(tick, "bid", 0.0) or 0.0)
        ask = float(getattr(tick, "ask", 0.0) or 0.0)

        price = None
        if last > 0:
            price = last
        elif bid > 0 and ask > 0:
            price = (bid + ask) / 2.0
        elif bid > 0:
            price = bid
        elif ask > 0:
            price = ask

        if price is None:
            err = f"price<=0: {symbol} | tick={tick}"
            self._record_fail(0, err)
            return None, {"err": err}

        now_sec = time.time()
        tick_time_sec = float(getattr(tick, "time", 0) or 0)
        age_sec = now_sec - tick_time_sec if tick_time_sec > 0 else 0.0
        tick_time_msc = int(getattr(tick, "time_msc", 0) or 0)

        last_msc = self._last_tick_msc.get(symbol)
        same_tick = last_msc is not None and tick_time_msc == last_msc
        self._last_tick_msc[symbol] = tick_time_msc

        self._last_fail = None
        return price, {
            "tick_time_msc": tick_time_msc,
            "age_sec": age_sec,
            "same_tick": same_tick,
        }

    def get_price(self, symbol: str, mode: str = "mid") -> Optional[float]:
        """Return the requested price from the tick."""
        if not self.ensure_symbol(symbol):
            return None

        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            code, msg = mt5.last_error()
            self._record_fail(code, msg)
            if code == IPC_FAIL:
                self._connected = False
            return None

        if mode == "last":
            last = float(getattr(tick, "last", 0.0) or 0.0)
            return last if last > 0 else None
        if mode == "bid":
            bid = float(getattr(tick, "bid", 0.0) or 0.0)
            return bid if bid > 0 else None
        if mode == "ask":
            ask = float(getattr(tick, "ask", 0.0) or 0.0)
            return ask if ask > 0 else None
        if mode == "mid":
            bid = float(getattr(tick, "bid", 0.0) or 0.0)
            ask = float(getattr(tick, "ask", 0.0) or 0.0)
            if bid > 0 and ask > 0:
                return (bid + ask) / 2.0
            return None

        raise ValueError(f"invalid price mode: {mode}")

    def get_close_series_d1(self, symbol: str, timeframe=mt5.TIMEFRAME_D1, bars: int = 200, shift: int = 1) -> list[float]:
        """Return a list of D1 closes for the closed candles."""
        self.ensure_connected()
        if not self.ensure_symbol(symbol):
            raise RuntimeError(f"symbol not available: {symbol}")

        rates = mt5.copy_rates_from_pos(symbol, timeframe, shift, bars)
        if rates is None or len(rates) == 0:
            code, msg = mt5.last_error()
            raise RuntimeError(f"copy_rates vazio (D1): {symbol} | {code} {msg}")

        return [float(r["close"]) for r in rates]

    def get_last_closed_close_d1(self, symbol: str) -> float:
        return self.get_close_series_d1(symbol, bars=1, shift=1)[0]

    def get_close_series(self, symbol: str, timeframe=mt5.TIMEFRAME_M1, bars: int = 200) -> list[float]:
        self.ensure_connected()
        if not self.ensure_symbol(symbol):
            raise RuntimeError(f"symbol not available: {symbol}")

        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, bars)
        if rates is None or len(rates) == 0:
            code, msg = mt5.last_error()
            raise RuntimeError(f"copy_rates vazio: {symbol} | {code} {msg}")

        return [float(x) for x in rates["close"]]

    def get_close_rates(self, symbol: str, timeframe, bars: int, shift: int = 1) -> list[dict]:
        self.ensure_connected()
        if not self.ensure_symbol(symbol):
            raise RuntimeError(f"symbol not available: {symbol}")

        rates = mt5.copy_rates_from_pos(symbol, timeframe, shift, bars)
        if rates is None or len(rates) == 0:
            code, msg = mt5.last_error()
            raise RuntimeError(f"copy_rates vazio: {symbol} | {code} {msg}")

        return [dict(r) for r in rates]

    def now_str(self) -> str:
        return datetime.now().isoformat(timespec="seconds")
