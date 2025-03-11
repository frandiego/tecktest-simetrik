{{
    config(
        schema = 'gold', 
        materialized = 'table', 
    )
}}


select 
    distinct 
    aircraft_id, 
    aircraft_model, 
    aircraft_mode_s, 
    aircraft_reg

from {{ ref('flights') }}

WHERE 
aircraft_model is not null 
AND 
aircraft_model != 'nan'

