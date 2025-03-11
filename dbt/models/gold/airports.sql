{{
    config(
        schema = 'gold' , 
        materialized = 'table', 
    )
}}


select 
    distinct 
    airport_name,
    airport_iata,
    airport_icao,
    airport_time_zone, 

from {{ ref('flights') }}

WHERE 
airport_iata is not null AND airport_iata != 'nan'

