{% set merge_bucket = var('merge_bucket', none) %}
{% set batch_id = var('batch_id', '') %}
{% set predicates = [] %}

{% if is_incremental() %}
    {% if merge_bucket is not integer
          or merge_bucket < 0
          or merge_bucket > 15 %}
        {{ exceptions.raise_compiler_error(
            "Provide merge_bucket as an integer from 0 to 15"
        ) }}
    {% endif %}

    {% if batch_id not in ['001', '002', '003'] %}
        {{ exceptions.raise_compiler_error(
            "Provide batch_id as a string: 001, 002, or 003"
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
    schema='gold',
    views_enabled=false,
    incremental_predicates=predicates,
    on_schema_change='fail'
) }}

select
    order_id,
    product_id,
    add_to_cart_order,
    reordered,
    'prior' as eval_set,
    _batch_id
from {{ ref('stg_order_products_prior_incremental') }}

{% if is_incremental() %}
where _batch_id = '{{ batch_id }}'
  and mod(order_id, 16) = {{ merge_bucket }}
{% endif %}

union all 

select
    order_id,
    product_id,
    add_to_cart_order,
    reordered,
    'train' as eval_set,
    _batch_id
from {{ ref('stg_order_products_train_incremental') }}

{% if is_incremental() %}
where _batch_id = '{{ batch_id }}'
  and mod(order_id, 16) = {{ merge_bucket }}
{% endif %}