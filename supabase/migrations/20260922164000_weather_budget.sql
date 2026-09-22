create table runwy_security.weather_budget(day date primary key, used integer not null);
alter table runwy_security.weather_budget enable row level security;
create function public.runwy_reserve_weather_budget(p_limit integer) returns boolean
language plpgsql security definer set search_path='' as $$
declare consumed integer; today date:=(now() at time zone 'UTC')::date;
begin
 insert into runwy_security.weather_budget as b(day,used) values(today,1)
 on conflict(day) do update set used=b.used+1 where b.used<greatest(1,least(p_limit,10000)) returning used into consumed;
 delete from runwy_security.weather_budget where day<today-7;
 return consumed is not null;
end $$;
revoke all on function public.runwy_reserve_weather_budget(integer) from public,anon,authenticated;
grant execute on function public.runwy_reserve_weather_budget(integer) to service_role;
