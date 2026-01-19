# Django -> MT5 Bridge (HTTP seguro)

## Variaveis de ambiente
- `MT5_BRIDGE_URL`: URL do mt5_bridge (ex: `http://IP_DA_VPS:8001`).
- `MT5_BRIDGE_API_KEY`: chave enviada no header `X-API-Key`.
- `MT5_DRY_RUN`: `True`/`False` (controla envio real de ordens no app).

## Uso no Django (exemplo)
Arquivo: `core/views.py` (ja existe `teste_mt5`)
```python
from django.http import JsonResponse
from mt5_bridge_client.mt5client import get_latest_price, MT5BridgeError

def teste_mt5(request):
    symbol = request.GET.get("symbol", "PETR4")
    try:
        price = get_latest_price(symbol)
        return JsonResponse({"ok": True, "symbol": symbol, "price": price})
    except MT5BridgeError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=502)
```

## Como testar
Local:
- Defina `MT5_BRIDGE_URL` e `MT5_BRIDGE_API_KEY` no `.env`.
- Rode o Django normalmente.

Fly.io:
- Defina os secrets no app Fly.
- Garanta que o `MT5_BRIDGE_URL` aponte para a VPS (nao use localhost).
