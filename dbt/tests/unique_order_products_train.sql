select
    order_id,
    product_id,
    count(*) as row_count
from {{ ref('stg_order_products_train_incremental') }}
group by order_id, product_id
having count(*) > 1