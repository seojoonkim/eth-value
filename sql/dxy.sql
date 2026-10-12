-- Nominal Broad U.S. Dollar Index (FRED series DTWEXBGS, Federal Reserve H.10, Jan 2006 = 100).
-- NOT the ICE DXY (6-currency, EUR-heavy): this is the Fed's trade-weighted index vs 26 currencies.
-- Rows come only from FRED's public CSV (scripts/data-collector.js collect_dxy). Business days only;
-- FRED's "." (holiday / no observation) is skipped, never filled. FRED publishes weekly (Mondays).
create table if not exists public.historical_dxy (
  date date primary key,
  index_value numeric not null,
  source text not null default 'fred',
  fetched_at timestamptz default now()
);
alter table public.historical_dxy enable row level security;
do $$ begin
  if not exists (select 1 from pg_policies where tablename='historical_dxy' and policyname='historical_dxy_public_read') then
    create policy historical_dxy_public_read on public.historical_dxy for select using (true);
  end if;
end $$;
grant select on public.historical_dxy to anon, authenticated;
select count(*) from public.historical_dxy;
