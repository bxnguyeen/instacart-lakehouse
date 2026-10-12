{{ config(
    materialized='table',
    schema='silver'
) }}

select 
    department_id,
    trim(department) as department,
    _catalog_version,
    _source_file,
    _ingested_at
from {{ source('instacart', 'departments') }}
where _catalog_version = '{{ var("catalog_version") }}'