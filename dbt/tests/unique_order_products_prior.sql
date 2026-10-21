{{ config(tags=['unique_prior']) }}

select
    order_id,
    product_id,
    count(*) as row_count
from {{ ref('stg_order_products_prior') }}
group by order_id, product_id
having count(*) > 1