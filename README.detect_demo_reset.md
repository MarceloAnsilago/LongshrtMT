# Detectando resets de conta demo MT5

1. **Pré-requisitos**
   - O terminal MT5 + FastAPI bridge (`mt5_bridge`) precisam estar rodando e acessíveis via `MT5_BRIDGE_URL`.
   - O Django deve conhecer essa URL e estar executando dentro do `venv` do projeto.

2. **Execução**

```bash
venv\\Scripts\\python.exe manage.py detect_demo_reset [--request-id=123e4567-e89b-12d3-a456-426614174000]
```

O comando varre trades marcados como abertos, compara com o `positions_get()` do bridge, busca `history_deals_get()` no intervalo `opened_at - 5 min` até `now + 2 min` e marca como `encerrado_reset_demo` qualquer perna sem posição/OUT e com idade mínima (default 180s). Cada trade inspecionado gera log JSON na forma:

```json
{
  "request_id": "opt",
  "operation_id": 42,
  "ticket": 1234,
  "position_id": 5678,
  "symbol": "PETR4",
  "in_db_open": true,
  "in_mt5_positions": false,
  "found_out_deal": false,
  "classification": "reset_demo_suspeito",
  "history_error": false
}
```

3. **Persistência**

- Trades marcados como reset têm `status="encerrado_reset_demo"`, `closed_at` atualizado e são registrados em `MT5IncidentEvent`, que guarda `account_info`, `positions_total`, janela usada e o payload resumido de tickets/out.
- Se todas as pernas de uma operação forem atualizadas, a operação como um todo vira `status="closed"`.

4. **Exemplo de saída**

```
Nenhum reset demo detectado.
```

ou

```
[2026-01-01T12:34:56.789000+00:00] Operação 7 (ticket=123456) marcada como reset demo.
```

5. **Testes**

```bash
venv\\Scripts\\python.exe manage.py test operacoes
```

O conjunto incluirá validação dos quatro cenários principais (reset sem deal, reset com deal, trade ainda presente e janela ainda curta).
