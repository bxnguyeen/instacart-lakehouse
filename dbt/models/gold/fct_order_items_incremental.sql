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
    schema='gold',
    views_enabled=false,
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

{% endif %}