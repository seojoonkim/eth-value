-- Relative value (section 03, relval.js): ETH vs SOL vs BTC P/Fees and P/Revenue.
-- Two single-source tables, one source each, never mixed or filled:
--   historical_chain_fees  ← DefiLlama /summary/fees/<chain> (dailyFees, dailyRevenue), complete UTC days only.
--   historical_chain_mcap  ← CoinGecko /coins/<id>/market_chart (market_caps), 00:00 UTC daily snapshots only.
-- A missing day stays missing. Revenue is DefiLlama's own per-chain definition (see its methodology).
create table if not exists public.historical_chain_fees (
  date date not null,
  chain text not null check (chain in ('ethereum','solana','bitcoin')),
  fees_usd numeric,
  revenue_usd numeric,
  source text not null default 'defillama',
  fetched_at timestamptz default now(),
  primary key (date, chain)
);
create table if not exists public.historical_chain_mcap (
  date date not null,
  chain text not null check (chain in ('ethereum','solana','bitcoin')),
  mcap_usd numeric not null,
  source text not null default 'coingecko',
  fetched_at timestamptz default now(),
  primary key (date, chain)
);
alter table public.historical_chain_fees enable row level security;
alter table public.historical_chain_mcap enable row level security;
do $$ begin
  if not exists (select 1 from pg_policies where tablename='historical_chain_fees' and policyname='historical_chain_fees_public_read') then
    create policy historical_chain_fees_public_read on public.historical_chain_fees for select using (true);
  end if;
  if not exists (select 1 from pg_policies where tablename='historical_chain_mcap' and policyname='historical_chain_mcap_public_read') then
    create policy historical_chain_mcap_public_read on public.historical_chain_mcap for select using (true);
  end if;
end $$;
grant select on public.historical_chain_fees to anon, authenticated;
grant select on public.historical_chain_mcap to anon, authenticated;
select (select count(*) from public.historical_chain_fees) as fees_rows, (select count(*) from public.historical_chain_mcap) as mcap_rows;
