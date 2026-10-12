-- Scheduled macro / protocol events shown as the "Upcoming" line in News Impact.
-- Rows come only from first-party schedules (scripts/calendar-collector.js):
--   fomc     federalreserve.gov FOMC calendar (date only; Fed marks dates tentative until confirmed)
--   cpi      bls.gov release calendar (bls.ics, 08:30 US-Eastern)
--   upgrade  blog.ethereum.org "Mainnet Announcement" posts (activation time in UTC)
-- No estimated dates: an upgrade whose mainnet date is "TBD" is not stored.
create table if not exists public.calendar_events (
  id text primary key,                 -- kind:yyyy-mm-dd[:slug]
  kind text not null check (kind in ('fomc','cpi','upgrade')),
  name text not null,                  -- e.g. "FOMC", "CPI (Sep 2026)", "Fusaka"
  date date not null,                  -- event day (FOMC: decision day = last meeting day)
  date_start date,                     -- FOMC first meeting day
  starts_at timestamptz,               -- exact time when the source states one
  sep boolean default false,           -- FOMC meeting with Summary of Economic Projections
  source text not null,
  source_url text not null,
  fetched_at timestamptz default now()
);
create index if not exists calendar_events_date_idx on public.calendar_events(date);
alter table public.calendar_events enable row level security;
do $$ begin
  if not exists (select 1 from pg_policies where tablename='calendar_events' and policyname='calendar_events_public_read') then
    create policy calendar_events_public_read on public.calendar_events for select using (true);
  end if;
end $$;
grant select on public.calendar_events to anon, authenticated;
select count(*) from public.calendar_events;
