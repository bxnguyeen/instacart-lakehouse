{% set batch_id = var('batch_id', '') %}
{% if is_incremental() and batch_id not in ['001', '002', '003'] %}
    {{ exceptions.raise_compiler_error(
        "Provide batch_id as a string: 001, 002, or 003"
    ) }}
{% endif %}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    schema='gold',
    views_enabled=false,
    on_schema_change='fail'
) }}

with selected_orders as (
    select
        order_id,
        user_id,
        eval_set,
        order_number,
        order_dow,
        order_hour_of_day,
        days_since_prior_order,
        _batch_id
    from {{ ref('stg_orders_incremental') }}

    {% if is_incremental() %}
    where _batch_id = '{{ batch_id }}'

    {% endif %}
),

item_summary as (
    select
        items.order_id,
        count(*) as item_count,
        sum(case when items.reordered = 1 then 1 else 0 end)
            as reordered_item_count
    from {{ ref('fct_order_items_incremental') }} as items
    inner join selected_orders as orders
        on items.order_id = orders.order_id


    group by items.order_id
)

select
    o.order_id,
    o.user_id,
    o.eval_set,
    o.order_number,
    o.order_dow,
    o.order_hour_of_day,
    o.days_since_prior_order,
    i.item_count,
    i.reordered_item_count,
    cast(i.reordered_item_count as double)
        / nullif(i.item_count, 0) as reorder_ratio,
    o._batch_id
from selected_orders as o
left join item_summary as i
    on o.order_id = i.order_id