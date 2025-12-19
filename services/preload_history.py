from __future__ import annotations

import MetaTrader5 as mt5
from time import sleep

from services.universe import UNIVERSE

SYMBOLS = UNIVERSE

TIMEFRAME = mt5.TIMEFRAME_D1
BARS = 220  # margem de segurança

def main():
    if not mt5.initialize():
        print("❌ mt5.initialize falhou:", mt5.last_error())
        return

    try:
        ok_count = 0
        for s in SYMBOLS:
            # garante símbolo selecionado no Market Watch
            if not mt5.symbol_select(s, True):
                print(f"⚠️ symbol_select falhou: {s} | {mt5.last_error()}")
                continue

            # tenta puxar rates algumas vezes (MT5 às vezes precisa “acordar”)
            got = False
            for attempt in range(1, 6):
                rates = mt5.copy_rates_from_pos(s, TIMEFRAME, 0, BARS)
                if rates is not None and len(rates) > 50:
                    got = True
                    ok_count += 1
                    print(f"✅ {s}: {len(rates)} candles carregados")
                    break
                else:
                    err = mt5.last_error()
                    print(f"⏳ {s}: tentativa {attempt}/5 sem dados ({err})")
                    sleep(0.4)

            if not got:
                print(f"❌ {s}: não consegui carregar histórico")

            sleep(0.2)

        print(f"\n📌 Preload finalizado. OK: {ok_count}/{len(SYMBOLS)}")
    finally:
        mt5.shutdown()

if __name__ == "__main__":
    main()
