{{ config(tags=['unique_prior']) }}

select
    order_id,
    product_id,
    count(*) as row_count
from {{ ref('stg_order_products_prior_incremental') }}
where mod(
    crc32(to_utf8(coalesce(cast(order_id as varchar), ''))),
    8
) = 0
group by order_id, product_id
having count(*) > 1
