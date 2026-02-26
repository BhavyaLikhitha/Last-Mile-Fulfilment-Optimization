# Data Quality & Testing Framework

> Last-Mile Fulfilment Optimization | dbt + Snowflake

---

## Testing Philosophy

Layered validation strategy across the pipeline:

| Layer              | Purpose                    | Test Type                       |
| ------------------ | -------------------------- | ------------------------------- |
| Staging (Bronze)   | Raw integrity validation   | Structural + Accounting checks  |
| Intermediate       | Business logic validation  | Transformation correctness      |
| Marts (Gold)       | KPI validation             | Business rule & metric bounds   |

All tests run via `dbt test`.

---

## 1. Structural Integrity Tests

### Not Null

Critical identifiers and dates must never be missing:

- `order_id`, `delivery_id`, `warehouse_id`, `product_id`, `date`, `experiment_id`

### Unique

Entity-level uniqueness enforced on:

- `stg_orders.order_id`
- `stg_deliveries.delivery_id`
- `stg_warehouses.warehouse_id`
- `stg_experiments.experiment_id`

### Referential Integrity (FK Tests)

- `int_order_enriched.assigned_warehouse_id` → `stg_warehouses.warehouse_id`
- `mart_delivery_performance.warehouse_id` → `stg_warehouses.warehouse_id`
- `mart_daily_product_kpis.product_id` → `stg_products.product_id`

---

## 2. Metric Bound Validation

All percentage KPIs standardized to **0–100 scale**.

| Column                       | Min | Max |
| ---------------------------- | --- | --- |
| `service_level_pct`          | 0   | 100 |
| `on_time_pct`                | 0   | 100 |
| `sla_breach_pct`             | 0   | 100 |
| `express_order_pct`          | 0   | 100 |
| `nearest_assignment_rate`    | 0   | 100 |
| `cross_region_pct`           | 0   | 100 |
| `allocation_efficiency_pct`  | 0   | 100 |
| `savings_pct`                | 0   | 100 |

**Why the change?** Models compute `round(value * 100.0 / total, 2)`, so bounds must be 0–100, not 0–1. This resolved multiple false failures.

---

## 3. Inventory Accounting Logic Fix

### Problem

Original test enforced strict accounting:

```
opening_stock + units_received + units_returned - units_sold = closing_stock
```

This failed for **3,281 rows**.

### Root Cause

Warehouses enforce a **zero-floor constraint** — inventory cannot go negative. When computed stock < 0, the system sets `closing_stock = 0` and `stockout_flag = TRUE`.

### Corrected Logic

```sql
closing_stock = greatest(
    opening_stock + units_received + units_returned - units_sold,
    0
)
```

### Updated Test

```sql
select *
from {{ ref('stg_inventory_snapshot') }}
where closing_stock != greatest(
    coalesce(opening_stock, 0)
    + coalesce(units_received, 0)
    + coalesce(units_returned, 0)
    - coalesce(units_sold, 0),
    0
)
```

---

## 4. Custom Business Logic Tests

### Cross-Region Allocation

Validates consistency between `assigned_warehouse_id`, `nearest_warehouse_id`, `allocation_result`, and `is_cross_region`.

### Experiment Significance

Validates `p_value` and control vs treatment comparison for statistical correctness.

### Cost Optimization Math

```
optimized_total_cost = baseline_total_cost * 0.92
savings_amount       = baseline_total_cost * 0.08
```

---

## 5. Capacity Stress Warning

Warehouse utilization can exceed 100% under stress.

- **Hard bound:** `0 ≤ capacity_utilization_pct ≤ 500`
- **Soft warning:** Flag rows > 110%

```sql
{{ config(severity='warn') }}

select *
from {{ ref('mart_daily_warehouse_kpis') }}
where capacity_utilization_pct > 110
```

Result: **8,776 rows** above 110% → `WARN` (expected). CI does not fail.

---

## 6. Revenue & Financial Consistency

- **Order vs Order Items:** `SUM(order_items) = orders.total_amount`
- **Delivery SLA Logic:** SLA breach flags, on-time calculations, time ordering consistency

---

## Test Execution Summary

| Metric | Value |
| ------ | ----- |
| Total  | 88    |
| Pass   | 87    |
| Warn   | 1 (capacity stress) |
| Error  | 0     |

---

## Standards Applied

- Percent metrics validated on correct scale (0–100)
- Accounting logic aligned to operational constraints (zero-floor)
- Hard failures only for true data corruption
- Soft warnings for business stress signals
- FK integrity enforced across all marts
- Custom domain tests for allocation, experiments, and cost optimization