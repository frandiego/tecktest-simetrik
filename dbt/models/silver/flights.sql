{{
    config(
        schema = 'silver', 
        materialized = 'table', 
        re_data_monitored=true,
    )
}}


select 
    id, 
    number, 
    code, 
    date::DATE as date, 
    flight_type, 
    status, 
    cast(case when codeshare_status == 'IsOperator' then 1 else 0 end as BOOL) as is_operator,
    cast(case when codeshare_status == 'IsCodeshared' then 1 else 0 end as BOOL) as is_codeshared, 
    is_cargo::BOOL as is_cargo, 
    call_sign, 
    airline_name,
    airline_iata,
    airline_icao,
    md5(aircraft_model || aircraft_mode_s || aircraft_reg) as aircraft_id, 
    md5(aircraft_model || aircraft_mode_s || aircraft_reg || is_cargo || airline_iata) as plane_id, 
    aircraft_model,
    aircraft_mode_s,
    aircraft_reg,
    terminal,
    quality, 
    gate, 
    airport_name,
    airport_iata,
    airport_icao,
    airport_time_zone, 
    strptime(scheduled_time_utc, '%Y-%m-%d %H:%M:%S%z') as  scheduled_time_utc,
    strptime(scheduled_time_local, '%Y-%m-%d %H:%M:%S%z') as  scheduled_time_local,
    strptime(revised_time_utc, '%Y-%m-%d %H:%M:%S%z') as  revised_time_utc,
    strptime(revised_time_local, '%Y-%m-%d %H:%M:%S%z') as  revised_time_local,
    strptime(runway_time_utc, '%Y-%m-%d %H:%M:%S%z') as  runway_time_utc, 
    strptime(runway_time_local, '%Y-%m-%d %H:%M:%S%z') as  runway_time_local
     
from {{ ref('data') }}


