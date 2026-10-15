-- Daily fair-value snapshots: the values the site itself showed at capture time.
-- Why: past fair values elsewhere on the site (charts, backtest, model pages) are RECOMPUTED with today's
-- formulas, constants and current circulating supply, so the number shown on a past day is not kept.
-- Rows are written only by scripts/fv-snapshot.cjs (run in model-pages.yml after each successful
-- collect-data run): headless browser opens the site, waits until all 12 model cards have a value,
-- and stores exactly what the cards / composite show. No backfill, no estimates: history starts on the
-- first capture day. A capture that is missing any model value is not stored.
create table if not exists public.fair_value_snapshots (
  captured_at timestamptz primary key,          -- when the page values were read (UTC)
  snapshot_date date not null,                  -- UTC day of captured_at
  composite_usd numeric not null,               -- headline composite fair value (mean of included models)
  models_included int not null,                 -- how many model cards the composite averaged
  models jsonb not null,                        -- { modelId: fair value USD } for all 12 models as shown
  eth_price_usd numeric not null,               -- ETH price the page used for the calculation
  price_is_live boolean not null,               -- true = live spot (CoinGecko/Coinbase), false = last stored close
  site_version text not null,                   -- version badge text, e.g. v1.0.0
  constants_commit text,                        -- commit of the latest MODEL_CONSTANTS changelog entry
  site_commit text,                             -- git commit of the page that computed the values
  source text not null default 'ethval-page'
);
create index if not exists fair_value_snapshots_date_idx on public.fair_value_snapshots (snapshot_date);
alter table public.fair_value_snapshots enable row level security;
do $$ begin
  if not exists (select 1 from pg_policies where tablename='fair_value_snapshots' and policyname='fair_value_snapshots_public_read') then
    create policy fair_value_snapshots_public_read on public.fair_value_snapshots for select using (true);
  end if;
end $$;
grant select on public.fair_value_snapshots to anon, authenticated;
select count(*) as rows from public.fair_value_snapshots;
