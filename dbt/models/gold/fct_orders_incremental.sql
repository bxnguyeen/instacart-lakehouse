{% set merge_bucket = var('merge_bucket', none) %}
{% set batch_id = var('batch_id', '') %}
{% set predicates = [] %}

{% if is_incremental() %}
    {% if merge_bucket is not integer
          or merge_bucket < 0
          or merge_bucket > 7 %}
        {{ exceptions.raise_compiler_error(
            "Provide merge_bucket as an integer from 0 to 7"
        ) }}
    {% endif %}

    {% if batch_id not in ['001', '002', '003'] %}
        {{ exceptions.raise_compiler_error(
            "Provide batch_id as a string: 001, 002, or 003"
        ) }}
    {% endif %}

    {% set predicates = [
        "mod(DBT_INTERNAL_DEST.order_id, 8) = " ~ merge_bucket
    ] %}
{% endif %}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    schema='gold',
    views_enabled=false,
    incremental_predicates=predicates,
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
      and mod(order_id, 8) = {{ merge_bucket }}
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

    {% if is_incremental() %}
    where mod(items.order_id, 8) = {{ merge_bucket }}
    {% endif %}

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