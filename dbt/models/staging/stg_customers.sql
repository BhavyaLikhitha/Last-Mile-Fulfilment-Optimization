/*
    stg_customers — Silver layer
    Source: SCD Type 2 snapshot (tracks segment changes)
    Transformations:
      - Reads from snapshot for full history
      - Casts types explicitly
      - Adds customer_tenure_days derived column
      - Adds lifetime_value_tier classification
      - Validates lat/long ranges
      - is_current flag for latest version
*/

with snapshot as (
    select * from {{ ref('snap_customer') }}
),

transformed as (
    select
        customer_id,
        cast(region as varchar(50)) as region,
        cast(city as varchar(50)) as city,
        cast(customer_segment as varchar(20)) as customer_segment,
        cast(order_frequency_score as decimal(3,2)) as order_frequency_score,
        cast(acquisition_date as date) as acquisition_date,
        cast(latitude as decimal(9,6)) as latitude,
        cast(longitude as decimal(9,6)) as longitude,

        -- Derived: customer tenure in days (from acquisition to snapshot date)
        datediff(day, acquisition_date, coalesce(dbt_valid_to, current_date())) as customer_tenure_days,

        -- Derived: engagement tier based on frequency score
        case
            when order_frequency_score >= 0.70 then 'High'
            when order_frequency_score >= 0.30 then 'Medium'
            else 'Low'
        end as engagement_tier,

        -- SCD Type 2 metadata
        dbt_valid_from,
        dbt_valid_to,
        case when dbt_valid_to is null then true else false end as is_current

    from snapshot
    where latitude between 24.0 and 50.0
      and longitude between -130.0 and -60.0
)

select * from transformed