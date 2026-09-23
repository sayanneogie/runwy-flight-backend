-- Trusted user imports: no tracking session, provider reference or worker job.
alter table public.user_flights
  add column import_metadata jsonb,
  add column source_record_id text;
alter table public.user_flights drop constraint user_flights_source_type_check;
alter table public.user_flights add constraint user_flights_source_type_check check (
  source_type in ('trip','manual_search','calendar_import','tracked','recovered','manual_verified',
    'manual_recovery','history_snapshot','history_repair','auto_archive','travelled_archive','flighty')
);
alter table public.user_flights add constraint user_flights_import_metadata_shape check (
  import_metadata is null or (jsonb_typeof(import_metadata) = 'object' and octet_length(import_metadata::text) <= 262144)
);
create unique index user_flights_flighty_source_unique
  on public.user_flights(user_id,source_record_id)
  where source_type='flighty' and source_record_id is not null and deleted_at is null;
create unique index user_flights_flighty_identity_unique
  on public.user_flights(user_id,(import_metadata->>'flightDate'),(import_metadata->>'airlineCode'),
    (import_metadata->>'flightNumber'),origin_iata,destination_iata)
  where source_type='flighty' and import_metadata is not null and deleted_at is null;

-- Compare both ICAO and IATA spellings using the incoming locally resolved
-- carrier aliases. Never query a flight provider or change the existing row.
create function public.runwy_import_number(p_value text, p_aliases jsonb)
returns text language plpgsql immutable set search_path='' as $$
declare value text:=regexp_replace(upper(coalesce(p_value,'')),'[[:space:]]','','g'); prefix text;
begin
  for prefix in select a.code from jsonb_array_elements_text(p_aliases) a(code) order by length(a.code) desc loop
    if left(value,length(prefix))=prefix and substring(value from length(prefix)+1) ~ '^[0-9]' then
      value:=substring(value from length(prefix)+1); exit;
    end if;
  end loop;
  return regexp_replace(value,'^0+([0-9])','\1');
end $$;
revoke all on function public.runwy_import_number(text,jsonb) from public,anon;
grant execute on function public.runwy_import_number(text,jsonb) to authenticated,service_role;

create function public.runwy_import_flighty_batch(p_flights jsonb)
returns table(input_id uuid,id uuid,inserted boolean)
language plpgsql security invoker set search_path='' as $$
declare actor uuid:=auth.uid(); item jsonb; m jsonb; existing_id uuid; inserted_id uuid;
  origin text; destination text; local_day date; zone text; aliases jsonb; dep timestamptz;
  validated_zones text[]:=array[]::text[];
begin
  if actor is null then raise exception 'Sign in to import flights' using errcode='42501'; end if;
  if jsonb_typeof(p_flights)<>'array' or jsonb_array_length(p_flights) not between 1 and 200 then
    raise exception 'Import batches must contain 1 to 200 flights' using errcode='22023'; end if;
  -- Same lock as ordinary user-flight writes. Cross-device retries and manual
  -- writes serialize before the duplicate query, within this transaction.
  perform pg_advisory_xact_lock(hashtextextended('runwy:account:'||actor::text,0));
  for item in select jsonb_array_elements(p_flights) loop
    m:=item->'metadata'; input_id:=(m->>'id')::uuid;
    origin:=upper(m->>'originIATA'); destination:=upper(m->>'destinationIATA');
    zone:=m->>'departureTimeZone'; local_day:=(m->>'flightDate')::date;
    aliases:=m->'airlineAliases';
    if m->>'source' is distinct from 'flighty' or m->>'sourceType' is distinct from 'import'
      or input_id is null or local_day is null or zone is null
      or origin is null or origin !~ '^[A-Z]{3}$' or destination is null or destination !~ '^[A-Z]{3}$'
      or coalesce(m->>'airlineCode','') !~ '^[A-Z0-9]{2,3}$'
      or coalesce(m->>'flightNumber','') !~ '^[0-9]{1,5}[A-Z]?$'
      or jsonb_typeof(aliases) is distinct from 'array' or jsonb_array_length(aliases) not between 1 and 3 then
      raise exception 'Invalid normalized import record' using errcode='22023';
    end if;
    if not aliases ? (m->>'airlineCode') or exists (
      select 1 from jsonb_array_elements_text(aliases) a(code) where a.code !~ '^[A-Z0-9]{2,3}$'
    ) then raise exception 'Invalid carrier aliases' using errcode='22023'; end if;
    if not zone=any(validated_zones) then
      if not exists(select 1 from pg_timezone_names where name=zone) then
        raise exception 'Unknown airport timezone' using errcode='22023'; end if;
      validated_zones:=array_append(validated_zones,zone);
    end if;
    select uf.id into existing_id from public.user_flights uf
      where uf.user_id=actor and uf.deleted_at is null and uf.source_type='flighty'
        and uf.source_record_id=lower(nullif(m->>'sourceRecordID','')) limit 1;
    if existing_id is null then
      select uf.id into existing_id from public.user_flights uf
        where uf.user_id=actor and uf.deleted_at is null and uf.source_type='flighty'
          and uf.import_metadata is not null
          and uf.import_metadata->>'flightDate'=m->>'flightDate'
          and uf.import_metadata->>'airlineCode'=m->>'airlineCode'
          and uf.import_metadata->>'flightNumber'=m->>'flightNumber'
          and uf.origin_iata=origin and uf.destination_iata=destination limit 1;
    end if;
    if existing_id is null then
      select uf.id into existing_id from public.user_flights uf
        where uf.user_id=actor and uf.deleted_at is null and uf.source_type<>'flighty'
          and upper(trim(uf.origin_iata))=origin and upper(trim(uf.destination_iata))=destination
          and (uf.scheduled_departure at time zone zone)::date=local_day
          and public.runwy_import_number(uf.display_flight_number,aliases)=m->>'flightNumber'
          and (exists(select 1 from jsonb_array_elements_text(aliases) a(code)
            where regexp_replace(upper(uf.display_flight_number),'[[:space:]]','','g') ~ ('^'||a.code||'[0-9]'))
            or upper(uf.marketing_airline_code) in(select jsonb_array_elements_text(aliases)))
        order by uf.created_at,uf.id limit 1;
    end if;
    if existing_id is not null then id:=existing_id; inserted:=false; return next; continue; end if;
    -- The legacy schema needs a date. For date-only exports this is a local
    -- midnight display anchor; nullable source times remain in import_metadata.
    dep:=coalesce((m->>'scheduledGateDeparture')::timestamptz,(m->>'actualGateDeparture')::timestamptz,
      (m->>'scheduledTakeoff')::timestamptz,(m->>'actualTakeoff')::timestamptz,local_day::timestamp at time zone zone);
    insert into public.user_flights as uf(
      id,user_id,source_type,lifecycle_state,display_flight_number,marketing_airline_code,marketing_airline_name,
      origin_iata,destination_iata,scheduled_departure,scheduled_arrival,actual_departure,actual_arrival,
      departure_terminal,departure_gate,arrival_terminal,arrival_gate,aircraft_type,status,
      tracked_snapshot,import_metadata,source_record_id,created_at,updated_at,notifications_enabled,notification_enabled,visibility
    ) values (
      input_id,actor,'flighty','archived',(m->>'airlineCode')||(m->>'flightNumber'),m->>'airlineCode',m->>'airlineName',
      origin,destination,dep,(m->>'scheduledGateArrival')::timestamptz,(m->>'actualGateDeparture')::timestamptz,(m->>'actualGateArrival')::timestamptz,
      m->>'departureTerminal',m->>'departureGate',m->>'arrivalTerminal',m->>'arrivalGate',m->>'aircraftTypeName',
      case when (m->>'cancelled')::boolean then 'cancelled' when m->>'divertedToIATA' is not null then 'diverted'
        when local_day < (now() at time zone zone)::date then 'landed' else 'scheduled' end,
      item->'tracked_snapshot',m,lower(nullif(m->>'sourceRecordID','')),now(),now(),false,false,'private'
    ) on conflict do nothing returning uf.id into inserted_id;
    if inserted_id is null then
      -- Do not acknowledge unrelated UUID collisions as an imported record.
      raise exception 'Flight identity changed; preview the import again' using errcode='40001';
    end if;
    id:=inserted_id; inserted:=true; return next;
  end loop;
end $$;
revoke all on function public.runwy_import_flighty_batch(jsonb) from public,anon;
grant execute on function public.runwy_import_flighty_batch(jsonb) to authenticated;

-- The legacy archive trigger replaces timestamp-equivalent rows. Trusted
-- imports use skip-only semantics and must never run that replacement branch.
do $$
declare definition text;
begin
  select pg_get_functiondef('public.reconcile_user_flight_history_occurrence()'::regprocedure) into definition;
  definition:=replace(definition, 'begin', E'begin\n  if new.source_type = ''flighty'' then return new; end if;');
  execute definition;
end $$;
