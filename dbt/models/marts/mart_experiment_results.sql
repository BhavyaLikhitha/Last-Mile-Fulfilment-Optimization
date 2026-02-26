/*
    mart_experiment_results — Gold layer
    Source: Intermediate + staging models
    Page: 6 (Experimentation & A/B Testing)
    Grain: One row per experiment per group (Control/Treatment)
    Note: p_value, confidence_intervals, is_significant filled by Python experimentation engine
*/

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
left join control_metrics c on em.experiment_id = c.experiment_id