{{
    config(
        schema = 'gold' , 
        materialized = 'table', 
    )
}}


select 
    distinct
    plane_id, 
    aircraft_id, 
    is_cargo, 
    airline_iata
    
from {{ ref('flights') }}


