with expected as (
    select count(*) as n
    from {{ ref('stg_orders_incremental') }}
),
actual as (
    select count(*) as n
    from {{ ref('fct_orders_incremental') }}
)

select e.n as expected_rows, a.n as actual_rows
from expected as e
cross join actual as a
where e.n <> a.n