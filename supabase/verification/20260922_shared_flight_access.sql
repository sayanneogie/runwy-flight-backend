-- Run after 20260922_secure_shared_flight_tables.sql as postgres.
-- Read-only assertions: no app data is selected or changed.
begin read only;

do $$
declare
  table_name text;
  client_role text;
  privilege_name text;
  qualified_table text;
begin
  if not exists (
    select 1 from pg_roles
    where rolname = 'service_role' and rolbypassrls
  ) then
    raise exception 'service_role must retain RLS bypass for backend access';
  end if;

  foreach table_name in array array[
    'flight_definitions', 'flight_instances', 'flight_instance_aliases',
    'flight_snapshots', 'flight_events', 'api_usage_logs'
  ] loop
    qualified_table := format('public.%I', table_name);

    if not exists (
      select 1 from pg_class
      where oid = qualified_table::regclass and relrowsecurity
    ) then
      raise exception 'RLS is not enabled on %', qualified_table;
    end if;

    foreach client_role in array array['anon', 'authenticated'] loop
      foreach privilege_name in array array[
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER'
      ] loop
        if has_table_privilege(client_role, qualified_table, privilege_name) then
          raise exception '% still has % on %', client_role, privilege_name, qualified_table;
        end if;
      end loop;

      -- Exercise the actual access check without returning any application data.
      execute format('set local role %I', client_role);
      begin
        execute format('select 1 from %s limit 0', qualified_table);
        raise exception '% unexpectedly read %', client_role, qualified_table;
      exception when insufficient_privilege then
        null;
      end;
      reset role;
    end loop;

    foreach privilege_name in array array['SELECT', 'INSERT', 'UPDATE', 'DELETE'] loop
      if not has_table_privilege('service_role', qualified_table, privilege_name) then
        raise exception 'service_role lost % on %', privilege_name, qualified_table;
      end if;
    end loop;

    set local role service_role;
    execute format('select 1 from %s limit 0', qualified_table);
    reset role;
  end loop;
end;
$$;

select 'PASS: six internal tables deny client access and retain service-role access' as result;
rollback;
