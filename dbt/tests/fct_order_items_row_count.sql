with expected as (
    select
        (select count(*)
        from {{ ref('stg_order_products_prior') }})
        + 
        (select count(*)
        from {{ ref('stg_order_products_train') }})
        as row_count
),

actual as (
    select count(*) as row_count
    from {{ ref('fct_order_items') }}
)

select 
    e.row_count as expected_rows,
    a.row_count as actual_rows
from expected as e 
cross join actual as a 
where e.row_count <> a.row_count