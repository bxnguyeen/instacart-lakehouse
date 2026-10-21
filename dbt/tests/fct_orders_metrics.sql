select *
from {{ ref('fct_orders') }}
where
    (
        eval_set in ('prior', 'train')
        and (
            item_count is null 
            or reordered_item_count is null
            or reorder_ratio is null 
            or item_count <= 0
            or reordered_item_count < 0
            or reordered_item_count > item_count
            or reorder_ratio < 0
            or reorder_ratio > 1
            or abs(
                reorder_ratio
                - cast(reordered_item_count as double)
                / nullif(item_count, 0)
            ) > 0.000000001
        )
    )
    or 
    (
        eval_set = 'test'
        and (
            item_count is not null
            or reordered_item_count is not null
            or reorder_ratio is not null
        )
    )