select
    order_id,
    product_id,
    count(*) as row_count
from {{ ref('stg_order_products_train') }}
group by order_id, product_id
having count(*) > 1