{% set merge_bucket = var('merge_bucket', none) %}
{% set predicates = [] %}

{% if is_incremental() %}
    {% if merge_bucket is not integer
          or merge_bucket < 0
          or merge_bucket > 15 %}
        {{ exceptions.raise_compiler_error(
            "Provide merge_bucket as an integer from 0 to 15"
        ) }}
    {% endif %}

    {% set predicates = [
        "mod(DBT_INTERNAL_DEST.order_id, 16) = " ~ merge_bucket
    ] %}
{% endif %}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['order_id', 'product_id'],
    schema='silver',
    views_enabled=false,
    incremental_predicates=predicates,
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
  and mod(cast(order_id as integer), 16) = {{ merge_bucket }}
{% endif %}