# EA MQL5 -> Supabase (Quotes)

This is a skeleton EA to publish quotes from MetaTrader 5 into Supabase.
It uses the Supabase REST API (PostgREST).

## Requirements

- Supabase project URL and a server key:
  - `SupabaseUrl` (example: `https://xyzcompany.supabase.co`)
  - `SupabaseKey` (service role key recommended for server-to-server)
- MT5 terminal must allow WebRequest:
  - Tools -> Options -> Expert Advisors -> Allow WebRequest for listed URL
  - Add your Supabase URL.

## Tables used

- `acoes_asset` (assets)
  - Columns: `id`, `ticker`
- `operacoes_operation` (open operations)
  - Columns used: `status`, `sell_asset_id`, `buy_asset_id`
- `cotacoes_quotelive` (live price)
  - Columns: `asset_id`, `price`, `updated_at`
- `cotacoes_quotedaily` (daily OHLC)
  - Columns: `asset_id`, `date`, `open`, `high`, `low`, `close`, `is_provisional`

## What the EA does

- Every `UpdateSeconds`:
  - Resolve the list of symbols:
    - If `UseOpenOperations = true`, it reads open operations from Supabase.
    - Otherwise it uses `SymbolsCsv`.
  - For each symbol:
    - Upsert into `cotacoes_quotelive` (price + updated_at).
    - Optionally upsert the last D1 candle into `cotacoes_quotedaily`.

## Notes and TODOs

- The EA skeleton does NOT include JSON parsing yet.
  - Implement a JSON parser or add a lightweight JSON helper.
  - Functions to implement: `ParseSingleId`, `ParseAssetIdsFromOperations`, `ParseTickers`.
- `updated_at` is required because Django sets it at app level.
- Use a service role key only on trusted servers (never inside the browser).
- Consider enabling RLS + a dedicated role if you want to limit access.

## Suggested next steps

1. Implement JSON parsing in the EA (or use a known MQL5 JSON helper).
2. Validate which tables/columns exist in Supabase.
3. Test a single symbol with `UseOpenOperations = false`.
4. After stable, switch `UseOpenOperations = true` to update only assets from open operations.
