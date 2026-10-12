with expected as (
    select
        count(*) as items,
        sum(case when reordered = 1 then 1 else 0 end) as reordered_items
    from {{ ref('fct_order_items_incremental') }}
),
actual as (
    select
        coalesce(sum(item_count), 0) as items,
        coalesce(sum(reordered_item_count), 0) as reordered_items
    from {{ ref('fct_orders_incremental') }}
)

select
    e.items as expected_items,
    a.items as actual_items,
    e.reordered_items as expected_reordered_items,
    a.reordered_items as actual_reordered_items
from expected as e
cross join actual as a
where e.items <> a.items
   or e.reordered_items <> a.reordered_items