select  
    p.order_id,
    p.product_id
from {{ ref('stg_order_products_prior') }} as p 
where exists (
    select 1
    from {{ ref('stg_order_products_train') }} as t 
    where t.order_id = p.order_id
        and t.product_id = p.product_id
)