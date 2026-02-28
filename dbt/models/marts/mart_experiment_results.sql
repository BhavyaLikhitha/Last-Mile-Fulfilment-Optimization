{# /*
    mart_experiment_results — Gold layer
    Source: Intermediate + staging models
    Page: 6 (Experimentation & A/B Testing)
    Grain: One row per experiment per group (Control/Treatment)
    Materialization: TABLE (full refresh) — needs full dataset for accurate control vs treatment comparison
    Note: p_value, confidence_intervals, is_significant filled by Python experimentation engine
*/

{{ config(
    materialized='table'
) }}

with assignments as (
    select * from {{ ref('stg_experiment_assignments') }}
),

orders as (
    select * from {{ ref('int_order_enriched') }}
),

deliveries as (
    select * from {{ ref('int_delivery_enriched') }}
),

experiment_metrics as (
    select
        a.experiment_id,
        a.group_name,
        count(distinct a.order_id) as total_orders,
        avg(d.actual_delivery_minutes) as avg_delivery_time_min,
        count(case when d.on_time_flag = true then 1 end) * 100.0
            / nullif(count(case when d.delivery_status = 'Delivered' then 1 end), 0) as service_level_pct,
        avg(o.total_fulfillment_cost) as avg_order_cost,
        sum(o.total_fulfillment_cost) as total_cost,
        avg(d.eta_accuracy_pct) as avg_eta_accuracy
    from assignments a
    join orders o on a.order_id = o.order_id
    left join deliveries d on o.order_id = d.order_id
    where o.order_status != 'Cancelled'
    group by 1, 2
),

control_metrics as (
    select * from experiment_metrics where group_name = 'Control'
)

select
    em.experiment_id,
    em.group_name,
    em.total_orders,
    round(em.avg_delivery_time_min, 2) as avg_delivery_time_min,
    round(em.service_level_pct, 2) as service_level_pct,
    round(em.avg_order_cost, 2) as avg_order_cost,
    0.0000 as stockout_rate,
    round(em.total_cost, 2) as total_cost,
    case
        when em.group_name = 'Treatment' and c.avg_order_cost > 0
        then round((em.avg_order_cost - c.avg_order_cost) * 100.0 / c.avg_order_cost, 2)
        else null
    end as lift_pct,
    round(em.avg_eta_accuracy, 2) as avg_eta_accuracy,
    null as p_value,
    null as confidence_interval_lower,
    null as confidence_interval_upper,
    null as is_significant

from experiment_metrics em
left join control_metrics c on em.experiment_id = c.experiment_id #}

/*
    mart_experiment_results — Gold layer
    Source: Intermediate + staging models
    Page: 6 (Experimentation & A/B Testing)
    Grain: One row per experiment per group (Control/Treatment)
    Materialization: TABLE (full refresh) — needs full dataset for accurate
                     control vs treatment comparison. Never incremental.

    Python-written columns (filled by experimentation engine):
      - p_value
      - confidence_interval_lower
      - confidence_interval_upper
      - is_significant
*/

{{ config(
    materialized='table'
) }}

with assignments as (
    select * from {{ ref('stg_experiment_assignments') }}
),

experiments as (
    select * from {{ ref('stg_experiments') }}
),

orders as (
    select * from {{ ref('int_order_enriched') }}
),

deliveries as (
    select * from {{ ref('int_delivery_enriched') }}
),

inventory as (
    select * from {{ ref('stg_inventory_snapshot') }}
),

/*
    Join orders to their experiment assignment records.
    An order is in scope only if it was explicitly assigned to an experiment.
*/
experiment_orders as (
    select
        a.experiment_id,
        a.group_name,
        a.order_id,
        o.total_fulfillment_cost,
        o.order_date
    from assignments a
    join orders o on a.order_id = o.order_id
    where o.order_status != 'Cancelled'
),

/*
    Delivery metrics per experiment group.
    Left join because not every order reaches Delivered status.
*/
experiment_deliveries as (
    select
        eo.experiment_id,
        eo.group_name,
        count(distinct eo.order_id)                                         as total_orders,
        avg(d.actual_delivery_minutes)                                      as avg_delivery_time_min,
        count(case when d.on_time_flag = true then 1 end) * 100.0
            / nullif(count(case when d.delivery_status = 'Delivered' then 1 end), 0)
                                                                            as service_level_pct,
        avg(eo.total_fulfillment_cost)                                      as avg_order_cost,
        sum(eo.total_fulfillment_cost)                                      as total_cost
    from experiment_orders eo
    left join deliveries d on eo.order_id = d.order_id
    group by 1, 2
),

/*
    Stockout rate per experiment group.
    Logic: for each experiment, find the target warehouses and date range from
    dim_experiments, then compute what fraction of product-warehouse-days had
    a stockout during that window.

    FIX: Original code hardcoded 0.0000 — this actually computes it from inventory.
*/
experiment_stockout as (
    select
        a.experiment_id,
        a.group_name,
        count(case when i.stockout_flag = true then 1 end) * 1.0
            / nullif(count(*), 0)                                           as stockout_rate
    from assignments a
    join orders o
        on a.order_id = o.order_id
    join inventory i
        on i.snapshot_date = o.order_date
        and i.warehouse_id = o.assigned_warehouse_id
    group by 1, 2
),

/*
    Control group metrics for lift calculation.
*/
control_metrics as (
    select
        experiment_id,
        avg_order_cost                                                       as control_avg_cost
    from experiment_deliveries
    where group_name = 'Control'
)

select
    ed.experiment_id,
    ed.group_name,
    ed.total_orders,
    round(ed.avg_delivery_time_min, 2)                                      as avg_delivery_time_min,
    round(ed.service_level_pct, 2)                                          as service_level_pct,
    round(ed.avg_order_cost, 2)                                             as avg_order_cost,
    round(coalesce(es.stockout_rate, 0), 4)                                 as stockout_rate,
    round(ed.total_cost, 2)                                                 as total_cost,
    case
        when ed.group_name = 'Treatment' and c.control_avg_cost > 0
        then round(
            (ed.avg_order_cost - c.control_avg_cost) * 100.0 / c.control_avg_cost,
            2
        )
        else null
    end                                                                     as lift_pct,
    null::decimal(8, 6)                                                     as p_value,
    null::decimal(8, 4)                                                     as confidence_interval_lower,
    null::decimal(8, 4)                                                     as confidence_interval_upper,
    null::boolean                                                           as is_significant

from experiment_deliveries ed
left join experiment_stockout es
    on ed.experiment_id = es.experiment_id
    and ed.group_name = es.group_name
left join control_metrics c
    on ed.experiment_id = c.experiment_id