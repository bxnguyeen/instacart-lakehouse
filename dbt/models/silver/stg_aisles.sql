{{ config(
    materialized='table',
    schema='silver'
) }}

select 
    aisle_id,
    trim(aisle) as aisle_name,
    _catalog_version,
    _source_file,
    _ingested_at
from {{ source('instacart', 'aisles') }}
where _catalog_version = '{{ var("catalog_version") }}'