{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    schema='silver',
    views_enabled=false,
    on_schema_change='fail'
) }}

select
    cast(order_id as integer) as order_id,
    cast(user_id as integer) as user_id,
    trim(eval_set) as eval_set,
    cast(order_number as integer) as order_number,
    cast(order_dow as integer) as order_dow,
    cast(order_hour_of_day as integer) as order_hour_of_day,
    cast(
        nullif(trim(days_since_prior_order),'')
        as decimal(12, 1)
    ) as days_since_prior_order,
    _batch_id,
    _manifest_id,
    _source_file,
    _ingested_at
from {{ source('instacart', 'orders_batches') }}

{% if is_incremental() %}
    {% set batch_id = var('batch_id', '') %}

    {% if batch_id not in ['001', '002', '003'] %}
        {{ exceptions.raise_compiler_error(
            "Provide batch_id as a string: 001, 002, or 003"
        ) }} 
    {% endif %}

where _batch_id = '{{ batch_id }}'
{% endif %}