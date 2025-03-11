{{
    config(
        schema = 'gold', 
        materialized = 'table', 
    )
}}


SELECT
    distinct
    id,
    aircraft_id, 
    plane_id, 
    number, 
    is_cargo,
    scheduled_time_utc,
    flight_type,
    status,
    airline_iata,
    code,
    airport_iata
FROM  {{ ref('flights') }}

ORDER BY scheduled_time_utc DESC


