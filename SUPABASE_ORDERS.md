# Supabase - Orders (MT5)

Run this SQL in Supabase (SQL Editor) to create the MT5 order queue tables and RPC.

```sql
create extension if not exists pgcrypto;

create table if not exists bridge_mt5terminal (
  terminal_id text primary key,
  last_seen_at timestamptz default now(),
  status text default 'online',
  meta jsonb
);

create index if not exists bridge_mt5terminal_last_seen_at_idx
  on bridge_mt5terminal (last_seen_at);

create table if not exists bridge_orderrequest (
  id uuid primary key default gen_random_uuid(),
  terminal_id text references bridge_mt5terminal(terminal_id),
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

create unique index if not exists bridge_orderrequest_terminal_client_unique
  on bridge_orderrequest (terminal_id, client_order_id);

create index if not exists bridge_orderrequest_terminal_status_created_idx
  on bridge_orderrequest (terminal_id, status, created_at);

create table if not exists bridge_orderevent (
  id bigserial primary key,
  order_id uuid references bridge_orderrequest(id) on delete cascade,
  event_type text not null,
  payload jsonb,
  created_at timestamptz default now()
);

create or replace function bridge_claim_next_order(p_terminal_id text)
returns setof bridge_orderrequest
language plpgsql
security definer
set search_path = public
as $$
begin
  return query
  with candidate as (
    select id
    from bridge_orderrequest
    where status = 'QUEUED'
      and bridge_orderrequest.terminal_id = p_terminal_id
    order by created_at asc
    for update skip locked
    limit 1
  )
  update bridge_orderrequest
  set status = 'CLAIMED',
      claimed_at = now()
  from candidate
  where bridge_orderrequest.id = candidate.id
  returning bridge_orderrequest.*;
end;
$$;
```

Notes:
- If you use RLS, add policies for `bridge_orderrequest`, `bridge_orderevent`, and `bridge_mt5terminal` or disable RLS for server-side usage.
- The MT5 EA uses PostgREST with the Service Role key.
