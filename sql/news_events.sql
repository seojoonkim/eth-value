create table if not exists public.news_events (
  id text primary key,
  date date not null,
  published_at timestamptz,
  category text not null,
  direction smallint not null check (direction in (-1,0,1)),
  impact smallint not null check (impact between 1 and 3),
  horizon text,
  metrics text[] default '{}',
  title jsonb not null,
  summary jsonb,
  mechanism jsonb,
  sources jsonb default '[]',
  confidence real,
  scheduled boolean default false,
  model text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);
create index if not exists news_events_date_idx on public.news_events(date desc);
alter table public.news_events enable row level security;
do $$ begin
  if not exists (select 1 from pg_policies where tablename='news_events' and policyname='news_events_public_read') then
    create policy news_events_public_read on public.news_events for select using (true);
  end if;
end $$;
grant select on public.news_events to anon, authenticated;
select count(*) from public.news_events;
