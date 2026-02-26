/*
    stg_shipments — Silver layer
    Source: RAW fact_shipments
    Transformations:
      - Type casting
      - Adds shipment_status derived from arrival dates
      - Adds cost_per_unit metric
      - Recalculates delay_days for consistency
*/

with source as (
    select * from {{ source('raw', 'fact_shipments') }}
),

transformed as (
    select
        cast(shipment_id as varchar(30)) as shipment_id,
        cast(supplier_id as varchar(20)) as supplier_id,
        cast(warehouse_id as varchar(20)) as warehouse_id,
        cast(product_id as varchar(20)) as product_id,
        cast(quantity as integer) as quantity,
        cast(shipment_cost as decimal(10,2)) as shipment_cost,
        cast(shipment_date as date) as shipment_date,
        cast(expected_arrival_date as date) as expected_arrival_date,
        cast(actual_arrival_date as date) as actual_arrival_date,
        cast(delay_flag as boolean) as delay_flag,
        cast(reorder_triggered_flag as boolean) as reorder_triggered_flag,

        -- Derived: recalculated delay days
        case
            when actual_arrival_date is not null
            then greatest(0, datediff(day, expected_arrival_date, actual_arrival_date))
            else null
        end as delay_days,

        -- Derived: shipment status
        case
            when actual_arrival_date is not null then 'Delivered'
            when expected_arrival_date < current_date() then 'Overdue'
            else 'In Transit'
        end as shipment_status,

        -- Derived: cost per unit
        case when quantity > 0 then round(shipment_cost / quantity, 2) else 0 end as cost_per_unit,

        -- Derived: transit duration
        case
            when actual_arrival_date is not null then datediff(day, shipment_date, actual_arrival_date)
            else null
        end as transit_days,

        -- Audit
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(batch_id as varchar(50)) as batch_id

    from source
    where quantity > 0
)

select * from transformed