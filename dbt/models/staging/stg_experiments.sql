/*
    stg_experiments — Silver layer
    Source: RAW (static experiment config)
    Transformations:
      - Type casting
      - Adds experiment_duration_days
      - Adds is_active flag
*/

with source as (
    select * from {{ source('raw', 'dim_experiments') }}
),

transformed as (
    select
        cast(experiment_id as varchar(20)) as experiment_id,
        cast(experiment_name as varchar(200)) as experiment_name,
        cast(strategy_name as varchar(100)) as strategy_name,
        cast(experiment_type as varchar(50)) as experiment_type,
        cast(description as varchar(500)) as description,
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date,
        cast(target_warehouses as varchar(200)) as target_warehouses,
        cast(status as varchar(20)) as status,

        -- Derived: experiment duration
        case
            when end_date is not null then datediff(day, start_date, end_date)
            else datediff(day, start_date, current_date())
        end as experiment_duration_days,

        -- Derived: is currently active
        case
            when status = 'Active' and (end_date is null or end_date >= current_date()) then true
            else false
        end as is_active

    from source
)

select * from transformed