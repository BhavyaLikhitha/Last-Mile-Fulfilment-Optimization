{# /*
    mart_delivery_performance — Gold layer
    Source: Intermediate enriched models
    Pages: 1 (Executive Overview), 4 (Last-Mile Delivery)
    Grain: One row per warehouse per day
    Incremental: merge on [date, warehouse_id]
*/

{{ config(
    materialized='incremental',
    unique_key=['date', 'warehouse_id'],
    incremental_strategy='merge'
) }}

with deliveries as (
    select * from {{ ref('int_delivery_enriched') }}
    {% if is_incremental() %}
    where delivery_date > (select max(date) from {{ this }})
    {% endif %}
),

driver_activity as (
    select * from {{ ref('stg_driver_activity') }}
    {% if is_incremental() %}
    where activity_date > (select max(date) from {{ this }})
    {% endif %}
),

daily_deliveries as (
    select
        delivery_date as date,
        warehouse_id,
        count(*) as total_deliveries,
        avg(case when delivery_status = 'Delivered' then actual_delivery_minutes end) as avg_delivery_time_min,
        avg(distance_km) as avg_distance_km,
        count(case when on_time_flag = true then 1 end) as on_time_count,
        count(case when delivery_status = 'Delivered' then 1 end) as delivered_count,
        count(case when sla_breach_flag = true then 1 end) as sla_breach_count,
        sum(delivery_cost) as total_delivery_cost,
        count(case when delivery_status = 'Failed' then 1 end) as failed_delivery_count,
        avg(eta_accuracy_pct) as avg_eta_accuracy_pct,
        avg(eta_error_minutes) as avg_eta_error_minutes,
        count(case when order_priority in ('Express', 'Same-Day') then 1 end) as express_count
    from deliveries
    group by 1, 2
),

daily_driver_util as (
    select
        activity_date as date,
        warehouse_id,
        avg(utilization_pct) as avg_driver_utilization
    from driver_activity
    group by 1, 2
)

select
    dd.date,
    dd.warehouse_id,
    dd.total_deliveries,
    round(dd.avg_delivery_time_min, 2) as avg_delivery_time_min,
    round(dd.avg_distance_km, 2) as avg_distance_km,
    case when dd.delivered_count > 0 then round(dd.on_time_count * 100.0 / dd.delivered_count, 2) else 0 end as on_time_pct,
    case when dd.total_deliveries > 0 then round(dd.sla_breach_count * 100.0 / dd.total_deliveries, 2) else 0 end as sla_breach_pct,
    round(dd.total_delivery_cost, 2) as total_delivery_cost,
    round(coalesce(du.avg_driver_utilization, 0), 2) as avg_driver_utilization,
    dd.failed_delivery_count,
    case when dd.total_deliveries > 0 then round(dd.express_count * 100.0 / dd.total_deliveries, 2) else 0 end as express_order_pct,
    round(dd.avg_eta_accuracy_pct, 2) as avg_eta_accuracy_pct,
    round(dd.avg_eta_error_minutes, 2) as avg_eta_error_minutes

from daily_deliveries dd
left join daily_driver_util du on dd.date = du.date and dd.warehouse_id = du.warehouse_id #}

/*
    mart_delivery_performance — Gold layer
    Source: Intermediate enriched models
    Pages: 1 (Executive Overview), 4 (Last-Mile Delivery)
    Grain: One row per warehouse per day
    Incremental: merge on [date, warehouse_id]

    ML-written columns (filled by predict_and_writeback.py):
      - predicted_eta       (AVG predicted delivery minutes per warehouse per day)
      - eta_error           (AVG actual - predicted per warehouse per day)
*/

{{ config(
    materialized='incremental',
    unique_key=['date', 'warehouse_id'],
    incremental_strategy='merge'
) }}

with deliveries as (
    select * from {{ ref('int_delivery_enriched') }}
    {% if is_incremental() %}
    where delivery_date > (select max(date) from {{ this }})
    {% endif %}
),

driver_activity as (
    select * from {{ ref('stg_driver_activity') }}
    {% if is_incremental() %}
    where activity_date > (select max(date) from {{ this }})
    {% endif %}
),

daily_deliveries as (
    select
        delivery_date                                                               as date,
        warehouse_id,
        count(*)                                                                    as total_deliveries,
        avg(case when delivery_status = 'Delivered'
            then actual_delivery_minutes end)                                       as avg_delivery_time_min,
        avg(distance_km)                                                            as avg_distance_km,
        count(case when on_time_flag = true then 1 end)                             as on_time_count,
        count(case when delivery_status = 'Delivered' then 1 end)                   as delivered_count,
        count(case when sla_breach_flag = true then 1 end)                          as sla_breach_count,
        sum(delivery_cost)                                                          as total_delivery_cost,
        count(case when delivery_status = 'Failed' then 1 end)                      as failed_delivery_count,
        avg(eta_accuracy_pct)                                                       as avg_eta_accuracy_pct,
        avg(eta_error_minutes)                                                      as avg_eta_error_minutes,
        count(case when order_priority in ('Express', 'Same-Day') then 1 end)       as express_count
    from deliveries
    group by 1, 2
),

daily_driver_util as (
    select
        activity_date                                                               as date,
        warehouse_id,
        avg(utilization_pct)                                                        as avg_driver_utilization
    from driver_activity
    group by 1, 2
)

select
    dd.date,
    dd.warehouse_id,
    dd.total_deliveries,
    round(dd.avg_delivery_time_min, 2)                                              as avg_delivery_time_min,
    round(dd.avg_distance_km, 2)                                                    as avg_distance_km,
    case when dd.delivered_count > 0
        then round(dd.on_time_count * 100.0 / dd.delivered_count, 2)
        else 0 end                                                                  as on_time_pct,
    case when dd.total_deliveries > 0
        then round(dd.sla_breach_count * 100.0 / dd.total_deliveries, 2)
        else 0 end                                                                  as sla_breach_pct,
    round(dd.total_delivery_cost, 2)                                                as total_delivery_cost,
    round(coalesce(du.avg_driver_utilization, 0), 2)                                as avg_driver_utilization,
    dd.failed_delivery_count,
    case when dd.total_deliveries > 0
        then round(dd.express_count * 100.0 / dd.total_deliveries, 2)
        else 0 end                                                                  as express_order_pct,
    -- Renamed from avg_eta_accuracy_pct / avg_eta_error_minutes.
    -- These are computed columns from int_delivery_enriched.
    -- The ML writeback script will MERGE updated values into predicted_eta and eta_error
    -- once the ETA model has run.
    round(dd.avg_eta_accuracy_pct, 2)                                               as predicted_eta,
    round(dd.avg_eta_error_minutes, 2)                                              as eta_error

from daily_deliveries dd
left join daily_driver_util du
    on dd.date = du.date
    and dd.warehouse_id = du.warehouse_id