/*
    mart_daily_product_kpis — Gold layer
    Source: Intermediate enriched models
    Pages: 2 (Inventory & Supply Chain), 3 (Demand Forecasting)
    Grain: One row per product per day
    Incremental: merge on [date, product_id]
*/

{{ config(
    materialized='incremental',
    unique_key=['date', 'product_id'],
    incremental_strategy='merge'
) }}

with inventory as (
    select * from {{ ref('int_inventory_enriched') }}
    {% if is_incremental() %}
    where snapshot_date > (select max(date) from {{ this }})
    {% endif %}
),

daily_inventory as (
    select
        snapshot_date as date,
        product_id,
        category,
        price_tier,
        sum(units_sold) as total_units_sold,
        avg(closing_stock) as avg_closing_stock,
        count(case when stockout_flag = true then 1 end) as stockout_count,
        sum(holding_cost) as total_holding_cost,
        sum(inventory_value) as total_inventory_value,
        avg(days_of_supply) as avg_days_of_supply
    from inventory
    group by 1, 2, 3, 4
),

daily_revenue as (
    select
        o.order_date as date,
        oi.product_id,
        sum(oi.revenue) as total_revenue,
        sum(oi.quantity) as total_quantity_ordered,
        sum(oi.discount_amount) as total_discounts
    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_orders') }} o on oi.order_id = o.order_id
    where o.order_status != 'Cancelled'
    {% if is_incremental() %}
    and o.order_date > (select max(date) from {{ this }})
    {% endif %}
    group by 1, 2
),

demand_volatility as (
    select
        snapshot_date as date,
        product_id,
        stddev(sum(units_sold)) over (
            partition by product_id
            order by snapshot_date
            rows between 6 preceding and current row
        ) as demand_volatility
    from {{ ref('stg_inventory_snapshot') }}
    group by snapshot_date, product_id
)

select
    i.date,
    i.product_id,
    i.category,
    i.price_tier,
    i.total_units_sold,
    round(coalesce(r.total_revenue, 0), 2) as total_revenue,
    i.stockout_count,
    round(i.avg_closing_stock, 2) as avg_closing_stock,
    case when i.avg_closing_stock > 0 then round(i.total_units_sold * 1.0 / i.avg_closing_stock, 2) else 0 end as inventory_turnover,
    round(i.avg_days_of_supply, 2) as avg_days_of_supply,
    round(i.total_holding_cost, 2) as total_holding_cost,
    round(i.total_inventory_value, 2) as total_inventory_value,
    null as demand_forecast,
    null as forecast_error,
    round(v.demand_volatility, 2) as demand_volatility

from daily_inventory i
left join daily_revenue r on i.date = r.date and i.product_id = r.product_id
left join demand_volatility v on i.date = v.date and i.product_id = v.product_id

{% if is_incremental() %}
where i.date > (select max(date) from {{ this }})
   or (i.date = (select max(date) from {{ this }}) and i.product_id not in (select product_id from {{ this }} where date = (select max(date) from {{ this }})))
{% endif %}