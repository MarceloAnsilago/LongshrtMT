# Supabase - Orders (MT5)

Run this SQL in Supabase (SQL Editor) to create the MT5 order queue tables and RPC.

```sql
create extension if not exists pgcrypto;

create table if not exists mt5_terminals (
  terminal_id text primary key,
  last_seen_at timestamptz default now(),
  status text default 'online',
  meta jsonb
);

create index if not exists mt5_terminals_last_seen_at_idx
  on mt5_terminals (last_seen_at);

create table if not exists order_requests (
  id uuid primary key default gen_random_uuid(),
  terminal_id text references mt5_terminals(terminal_id),
  pair_id text not null,
  side text not null,
  symbol_a text not null,
  symbol_b text,
  qty_a numeric not null,
  qty_b numeric,
  order_type text default 'MARKET',
  status text default 'QUEUED',
  client_order_id text not null,
  created_at timestamptz default now(),
  claimed_at timestamptz,
  done_at timestamptz,
  error text
);

create unique index if not exists order_requests_terminal_client_unique
  on order_requests (terminal_id, client_order_id);

create index if not exists order_requests_terminal_status_created_idx
  on order_requests (terminal_id, status, created_at);

create table if not exists order_events (
  id bigserial primary key,
  order_id uuid references order_requests(id) on delete cascade,
  event_type text not null,
  payload jsonb,
  created_at timestamptz default now()
);

create or replace function claim_next_order(terminal_id text)
returns setof order_requests
language plpgsql
security definer
set search_path = public
as $$
begin
  return query
  with candidate as (
    select id
    from order_requests
    where status = 'QUEUED'
      and order_requests.terminal_id = claim_next_order.terminal_id
    order by created_at asc
    for update skip locked
    limit 1
  )
  update order_requests
  set status = 'CLAIMED',
      claimed_at = now()
  from candidate
  where order_requests.id = candidate.id
  returning order_requests.*;
end;
$$;
```

Notes:
- If you use RLS, add policies for `order_requests`, `order_events`, and `mt5_terminals` or disable RLS for server-side usage.
- The MT5 EA uses PostgREST with the Service Role key.
