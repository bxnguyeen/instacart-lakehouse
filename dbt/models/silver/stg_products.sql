{{ config(
    materialized='table',
    schema='silver'
) }}

select
    product_id,
    trim(product_name) as product_name,
    aisle_id,
    department_id,
    _catalog_version,
    _source_file,
    _ingested_at
from {{ source('instacart', 'products') }}
where _catalog_version = '{{ var("catalog_version") }}'