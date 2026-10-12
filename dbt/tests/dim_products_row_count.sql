with source_count as (
    select count(*) as row_count
    from {{ ref('stg_products') }}
),

gold_count as (
    select count(*) as row_count
    from {{ ref('dim_products') }}
)

select  
    s.row_count as source_rows,
    g.row_count as gold_rows
from source_count as s 
cross join gold_count as g 
where s.row_count <> g.row_count