-- Realtime remains on the small public projection. Full provider data is server-only.
create table runwy_security.live_snapshot_payloads (
 tracking_session_id uuid primary key references public.tracking_sessions(id) on delete cascade,
 canonical_snapshot_json jsonb not null default '{}', raw_provider_payload_json jsonb not null default '{}',
 metrics_json jsonb not null default '{}'
);
alter table runwy_security.live_snapshot_payloads enable row level security;
grant select,insert,update,delete on runwy_security.live_snapshot_payloads to service_role;
create function public.runwy_basic_snapshot(value jsonb) returns jsonb
language plpgsql immutable set search_path='' as $$
declare result jsonb; k text; v jsonb;
begin
 if jsonb_typeof(value)='array' then
  select coalesce(jsonb_agg(public.runwy_basic_snapshot(x.value)),'[]'::jsonb) into result from jsonb_array_elements(value) x;
  return result;
 elsif jsonb_typeof(value)='object' then
  result:='{}';
  for k,v in select * from jsonb_each(value) loop
   if k in('trackPoints','track_points') then v:='[]'::jsonb;
   elsif k in('inboundFlight','livePosition','position','position_lat','position_lon','rawProviderResponse','rawProviderPayload',
    'raw_provider_payload_json','raw_provider_response','live_position','inbound_flight') then v:='null'::jsonb;
   else v:=public.runwy_basic_snapshot(v); end if;
   result:=result||jsonb_build_object(k,v);
  end loop;
  return result;
 end if;
 return value;
end $$;
revoke all on function public.runwy_basic_snapshot(jsonb) from public,anon,authenticated;
grant execute on function public.runwy_basic_snapshot(jsonb) to service_role;
insert into runwy_security.live_snapshot_payloads
select tracking_session_id,canonical_snapshot_json,raw_provider_payload_json,metrics_json from public.live_snapshots;
update public.live_snapshots set canonical_snapshot_json=public.runwy_basic_snapshot(canonical_snapshot_json),
 raw_provider_payload_json='{}',metrics_json=public.runwy_basic_snapshot(metrics_json),alerts_json=public.runwy_basic_snapshot(alerts_json);
create function public.runwy_project_live_snapshot() returns trigger
language plpgsql security definer set search_path='' as $$
begin
 new.canonical_snapshot_json:=public.runwy_basic_snapshot(new.canonical_snapshot_json);
 new.raw_provider_payload_json:='{}'; new.metrics_json:=public.runwy_basic_snapshot(new.metrics_json);
 new.alerts_json:=public.runwy_basic_snapshot(new.alerts_json);
 return new;
end $$;
revoke all on function public.runwy_project_live_snapshot() from public,anon,authenticated;
create trigger project_live_snapshot before insert or update of canonical_snapshot_json,raw_provider_payload_json,metrics_json
on public.live_snapshots for each row execute function public.runwy_project_live_snapshot();
