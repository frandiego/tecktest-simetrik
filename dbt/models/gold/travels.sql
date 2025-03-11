{{
    config(
        schema = 'gold', 
        materialized = 'table', 
    )
}}


select 
    distinct
    number, 
    code, 
    airport_iata,
    airline_iata 

from {{ ref('flights') }}

where number is not null and number != ''
