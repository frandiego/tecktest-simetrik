{{
    config(
        schema = 'gold', 
        materialized = 'table', 
    )
}}


select 
    distinct 
    id,
    date,
    scheduled_time_utc,
    scheduled_time_local,
    revised_time_utc,
    revised_time_local,
    runway_time_utc, 
    runway_time_local

from {{ ref('flights') }}

