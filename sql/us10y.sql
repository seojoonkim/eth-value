-- US 10-year Treasury constant-maturity yield (FRED series DGS10, Board of Governors H.15).
-- Rows come only from FRED's public CSV (scripts/data-collector.js collect_us10y). Business days only;
-- FRED's "." (holiday / no observation) is skipped, never filled.
create table if not exists public.historical_us10y (
  date date primary key,
  yield_pct numeric not null,          -- percent, e.g. 5.22
  source text not null default 'fred',
  fetched_at timestamptz default now()
);
alter table public.historical_us10y enable row level security;
do $$ begin
  if not exists (select 1 from pg_policies where tablename='historical_us10y' and policyname='historical_us10y_public_read') then
    create policy historical_us10y_public_read on public.historical_us10y for select using (true);
  end if;
end $$;
grant select on public.historical_us10y to anon, authenticated;
select count(*) from public.historical_us10y;
