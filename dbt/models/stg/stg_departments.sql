{{ config(
    materialized='table',
    schema='02_stg'
) }}

select 
    department_id,
    trim(department) as department
from {{ source('instacart', 'departments') }}