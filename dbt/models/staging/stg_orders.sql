/*
    stg_orders — Silver layer
    Source: RAW fact_orders
    Transformations:
      - Type casting
      - NULL handling for experiment fields
      - Adds is_cross_region flag (assigned != nearest)
      - Adds order_value_tier classification
      - Filters out test/invalid orders (total_amount <= 0)
*/

with source as (
    select * from {{ source('raw', 'fact_orders') }}
),

transformed as (
    select
        cast(order_id as varchar(30)) as order_id,
        cast(order_date as date) as order_date,
        cast(order_timestamp as timestamp) as order_timestamp,
        cast(customer_id as varchar(20)) as customer_id,
        cast(assigned_warehouse_id as varchar(20)) as assigned_warehouse_id,
        cast(nearest_warehouse_id as varchar(20)) as nearest_warehouse_id,
        cast(allocation_strategy as varchar(50)) as allocation_strategy,
        cast(order_priority as varchar(20)) as order_priority,
        cast(total_items as integer) as total_items,
        cast(total_amount as decimal(10,2)) as total_amount,
        cast(total_fulfillment_cost as decimal(12,2)) as total_fulfillment_cost,
        cast(order_status as varchar(20)) as order_status,
        cast(return_flag as boolean) as return_flag,
        cast(nullif(experiment_id, '') as varchar(20)) as experiment_id,
        cast(nullif(experiment_group, '') as varchar(20)) as experiment_group,

        -- Derived: was this order sent to a non-nearest warehouse?
        case
            when assigned_warehouse_id != nearest_warehouse_id then true
            else false
        end as is_cross_region,

        -- Derived: order value tier
        case
            when total_amount >= 500 then 'High Value'
            when total_amount >= 100 then 'Medium Value'
            else 'Low Value'
        end as order_value_tier,

        -- Derived: is this order in an experiment?
        case when experiment_id is not null and experiment_id != '' then true else false end as is_experiment_order,

        -- Audit
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(batch_id as varchar(50)) as batch_id

    from source
    where total_amount > 0
      and order_status is not null
)

select * from transformed