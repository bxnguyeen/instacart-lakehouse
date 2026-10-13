{% set batch_id = var('batch_id', '') %}
{% if is_incremental() and batch_id not in ['001', '002', '003'] %}
    {{ exceptions.raise_compiler_error(
        "Provide batch_id as a string: 001, 002, or 003"
    ) }}
{% endif %}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['order_id', 'product_id'],
    schema='silver',
    views_enabled=false,
    on_schema_change='fail'
) }}

select
    cast(order_id as integer) as order_id,
    cast(product_id as integer) as product_id,
    cast(add_to_cart_order as integer) as add_to_cart_order,
    cast(reordered as integer) as reordered,
    _batch_id,
    _manifest_id,
    _source_file,
    _ingested_at
from {{ source('instacart', 'order_products_prior_batches') }}

{% if is_incremental() %}
    {% set batch_id = var('batch_id', '') %}

    {% if batch_id not in ['001', '002', '003'] %}
        {{ exceptions.raise_compiler_error(
            "Provide batch_id as a string: 001, 002, or 003"
        ) }}
    {% endif %}

where _batch_id = '{{ batch_id }}'

{% endif %}