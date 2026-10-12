with expected as (
    select
        (select count(*)
        from {{ ref('stg_order_products_prior_incremental') }})
        + 
        (select count(*)
        from {{ ref('stg_order_products_train_incremental') }})
        as row_count
),

actual as (
    select count(*) as row_count
    from {{ ref('fct_order_items_incremental') }}
)

select 
    e.row_count as expected_rows,
    a.row_count as actual_rows
from expected as e 
cross join actual as a 
where e.row_count <> a.row_count