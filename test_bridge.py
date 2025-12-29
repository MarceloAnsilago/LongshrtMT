#!/usr/bin/env python3

import sys
from typing import Any

import httpx

PING_URL = "http://127.0.0.1:9000/api/ping"
RATES_URL = "http://127.0.0.1:9000/api/rates"
DEBUG_URL = "http://127.0.0.1:9000/api/debug/mt5_last_error"
PAYLOAD = {"symbol": "PETR4", "timeframe": "D1", "count": 10}


def _print_bars(data: Any) -> None:
    rates = data.get("rates")
    if not rates:
        print("Nenhum dado retornado.")
        return
    for rate in rates:
        ts = rate.get("time")
        o = rate.get("open")
        c = rate.get("close")
        print(f"time: {ts} open: {o} close: {c}")


def main() -> None:
    try:
        resp = httpx.get(PING_URL, timeout=10.0)
    except httpx.RequestError as exc:
        print(f"Bridge OFF ({exc})")
        sys.exit(1)

    if resp.status_code != 200:
        print("Bridge OFF")
        sys.exit(1)

    print("Bridge OK")
    print("Recebendo dados...")

    try:
        resp = httpx.post(RATES_URL, json=PAYLOAD, timeout=15.0)
    except httpx.RequestError as exc:
        print(f"Erro inesperado: {exc}")
        sys.exit(1)

    if resp.status_code != 200:
        print(f"status_code: {resp.status_code}")
        print(f"response text: {resp.text}")
        if resp.status_code == 500:
            try:
                debug_resp = httpx.get(DEBUG_URL, timeout=10.0)
            except httpx.RequestError as exc:
                print(f"DEBUG MT5 LAST ERROR (falha): {exc}")
            else:
                print("DEBUG MT5 LAST ERROR")
                print(f"  status_code: {debug_resp.status_code}")
                print(f"  body: {debug_resp.text}")
    else:
        try:
            data = resp.json()
        except ValueError:
            print("Falha ao decodificar JSON")
        else:
            _print_bars(data)

    print("\nTESTE FINALIZADO")


if __name__ == "__main__":
    main()
