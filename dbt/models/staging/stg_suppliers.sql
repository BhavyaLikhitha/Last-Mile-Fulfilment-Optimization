/*
    stg_suppliers — Silver layer
    Source: SCD Type 2 snapshot (tracks reliability and lead time changes)
    Transformations:
      - Reads from snapshot for full history
      - Adds supplier_grade classification
      - Validates reliability score range
*/

with snapshot as (
    select * from {{ ref('snap_supplier') }}
),

transformed as (
    select
        supplier_id,
        cast(supplier_name as varchar(100)) as supplier_name,
        cast(region as varchar(50)) as region,
        cast(average_lead_time as integer) as average_lead_time,
        cast(lead_time_std_dev as decimal(5,2)) as lead_time_std_dev,
        cast(reliability_score as decimal(3,2)) as reliability_score,
        cast(product_categories as varchar(200)) as product_categories,

        -- Derived: supplier grade based on reliability
        case
            when reliability_score >= 0.93 then 'A'
            when reliability_score >= 0.85 then 'B'
            when reliability_score >= 0.75 then 'C'
            else 'D'
        end as supplier_grade,

        -- Derived: lead time category
        case
            when average_lead_time <= 3 then 'Fast'
            when average_lead_time <= 5 then 'Standard'
            else 'Slow'
        end as lead_time_category,

        -- SCD Type 2 metadata
        dbt_valid_from,
        dbt_valid_to,
        case when dbt_valid_to is null then true else false end as is_current

    from snapshot
    where reliability_score between 0 and 1
)

select * from transformed