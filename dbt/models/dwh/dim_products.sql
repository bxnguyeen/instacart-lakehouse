{{ config(
    materialized='table',
    schema='03_dwh'
) }}

select
    p.product_id,
    p.product_name,
    p.aisle_id,
    a.aisle_name,
    p.department_id,
    d.department
from {{ ref('stg_products') }} as p
left join {{ ref('stg_aisles') }} as a
    on p.aisle_id = a.aisle_id
left join {{ ref('stg_departments') }} as d
    on p.department_id = d.department_id