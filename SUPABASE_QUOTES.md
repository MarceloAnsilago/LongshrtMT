# Supabase - Cotacoes (Django read-only)

## Tabelas usadas pelo Django
O Django ja esta conectado ao Supabase via `DATABASE_URL`. Ele le direto das tabelas abaixo:

- `acoes_asset`
  - `id` (PK), `ticker`, `ticker_yf`, `name`, `is_active`, `logo_prefix`
- `cotacoes_quotedaily`
  - `asset_id` (FK -> `acoes_asset.id`)
  - `date` (date, unique por asset)
  - `open`, `high`, `low`, `close`, `is_provisional`
- `cotacoes_quotelive`
  - `asset_id` (FK -> `acoes_asset.id`, unique)
  - `price`
  - `updated_at`

## Contrato para o EA (MQL5)
1) Para cada ativo, garantir que existe em `acoes_asset`.
2) Inserir/atualizar **D1** em `cotacoes_quotedaily` (ultimas 210 linhas).
3) Atualizar o ultimo preco em `cotacoes_quotelive` para ativos dos cards/operacoes abertas.

## Upsert sugerido (Postgres)
```sql
-- D1 (cotacoes_quotedaily)
INSERT INTO cotacoes_quotedaily (asset_id, date, open, high, low, close, is_provisional)
VALUES (:asset_id, :date, :open, :high, :low, :close, FALSE)
ON CONFLICT (asset_id, date) DO UPDATE
SET open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    is_provisional = FALSE;

-- Live (cotacoes_quotelive)
INSERT INTO cotacoes_quotelive (asset_id, price, updated_at)
VALUES (:asset_id, :price, NOW())
ON CONFLICT (asset_id) DO UPDATE
SET price = EXCLUDED.price,
    updated_at = NOW();
```

## Observacoes
- O Django nao escreve nesses dados; apenas le.
- Se um ativo novo for adicionado, o EA deve inserir as ultimas 210 cotacoes D1.
- Use timestamps em UTC para consistencia.
