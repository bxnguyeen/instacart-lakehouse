{{ config(
    materialized='table',
    schema='02_stg'
) }}

select 
    aisle_id,
    trim(aisle) as aisle_name
from {{ source('instacart', 'aisles') }}