/*
    int_inventory_enriched — Intermediate layer
    Enriches inventory snapshots with product details and warehouse capacity.
    Single source for all inventory analysis before mart aggregation.
*/

with inventory as (
    select * from {{ ref('stg_inventory_snapshot') }}
),

products as (
    select * from {{ ref('stg_products') }}
    where is_current = true
),

warehouses as (
    select warehouse_id, warehouse_name, region, capacity_units, capacity_tier
    from {{ ref('stg_warehouses') }}
)

select
    i.snapshot_date,
    i.warehouse_id,
    w.warehouse_name,
    w.region as warehouse_region,
    w.capacity_units as warehouse_capacity,
    w.capacity_tier as warehouse_capacity_tier,
    i.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.cost_price,
    p.selling_price,
    p.profit_margin_pct,
    p.price_tier,
    p.lead_time_days,
    p.reorder_point,
    p.safety_stock,
    p.lead_time_risk,
    i.opening_stock,
    i.units_sold,
    i.units_received,
    i.units_returned,
    i.closing_stock,
    i.stockout_flag,
    i.below_safety_stock_flag,
    i.reorder_triggered_flag,
    i.units_on_order,
    i.days_of_supply,
    i.holding_cost,
    i.inventory_value,
    i.net_stock_movement,
    i.inventory_health,

    -- Derived: capacity utilization for this product at this warehouse
    case
        when w.capacity_units > 0 then round(i.closing_stock * 100.0 / w.capacity_units, 4)
        else 0
    end as product_capacity_pct,

    -- Derived: stock coverage status
    case
        when i.closing_stock = 0 then 'Out of Stock'
        when i.closing_stock < p.safety_stock then 'Below Safety Stock'
        when i.closing_stock < p.reorder_point then 'Below Reorder Point'
        else 'Adequate'
    end as stock_coverage_status,

    -- Derived: potential revenue at risk (if stockout)
    case
        when i.stockout_flag = true then round(p.selling_price * i.units_sold, 2)
        else 0
    end as revenue_at_risk,

    i.batch_id

from inventory i
left join products p on i.product_id = p.product_id
left join warehouses w on i.warehouse_id = w.warehouse_id