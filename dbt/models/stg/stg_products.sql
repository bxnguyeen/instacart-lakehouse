{{ config(
    materialized='table',
    schema='02_stg'
) }}

select
    product_id,
    trim(product_name) as product_name,
    aisle_id,
    department_id
from {{ source('instacart', 'products') }}