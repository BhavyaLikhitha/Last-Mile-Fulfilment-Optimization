## dbt

- dbt debug
- dbt init
- dbt snapshot
- dbt run
- dbt test
- dbt docs serve

- add incremental for tables and marts

- dbt test --select tag:staging
- dbt test --select tag:revenue
- dbt test --select test_type:singular
- dbt test --select test_type:generic


 -m ml.training.predict_and_writeback --phase future_demand
============================================================
  FULFILLMENT PLATFORM — PREDICTION & WRITEBACK
  Phases: future_demand
============================================================

============================================================
  FUTURE DEMAND FORECAST — PREDICT & INSERT
============================================================
  Model loaded: C:\Users\bhavy\OneDrive\Desktop\GITHUB\BHAVYA\Last-Mile-Fulfilment-Optimization\ml\saved_models\demand_best.joblib
  Model  : demand_best
  Horizon: 180 days

  Historical data ends : 2025-02-01
  Forecast window      : 2025-02-02 → 2025-07-31

  Pulling seed data (2024-12-03 → 2025-02-01) for lag features...
  Loaded 30,500 seed rows
  Loading product attributes from stg_products...
  Loaded 500 products

  Building future date spine...
  180 days × 500 products = 90,000 rows
  Extended dates dimension to 2025-07-31 (+180 rows)
  Building features for future rows...
  Scoring 90,000 future rows

  Sample future predictions:
      date product_id  demand_forecast  forecast_horizon
2025-02-02  PROD-0001             0.00                30
2025-02-03  PROD-0001             0.28                30
2025-02-04  PROD-0001             0.16                30
2025-02-05  PROD-0001             0.21                30
2025-02-06  PROD-0001             0.00                30
2025-02-07  PROD-0001             0.00                30
2025-02-08  PROD-0001             2.52                30
2025-02-09  PROD-0001             4.33                30
2025-02-10  PROD-0001             4.64                30
2025-02-11  PROD-0001             4.63                30

  Horizon distribution:
     30d bucket:   15,000 rows
     60d bucket:   15,000 rows
     90d bucket:   15,000 rows
    180d bucket:   45,000 rows

  Cleared 0 existing future rows

  ✓ Inserted 90,000 future forecast rows
  ✓ Completed in 15s

============================================================
  All predictions complete in 15s
============================================================
(base) (last-mile-fulfilment-optimization-py3.11) PS C:\Users\bhavy\OneDrive\Desktop\GITHUB\BHAVYA\Last-Mile-Fulfilment-Optimization> python -m optimization.run_optimization
============================================================
  FULFILLMENT PLATFORM — OPTIMIZATION ENGINE
  Mode: full
============================================================

============================================================
  COST OPTIMIZATION — BASELINE VS OPTIMIZED
============================================================

  Loading mart_daily_warehouse_kpis...
  Loaded 8,776 rows
  Computing baseline costs...
  Applying optimization model...
  Computing allocation efficiency...

  Cost optimization summary:
    Total baseline cost : $2,690,751,556.68
    Total optimized cost: $2,469,919,086.14
    Total savings       : $ 220,832,470.54
    Avg savings pct     :           8.22%
    Avg allocation eff  :          81.64%

  ✓ Merged 8,776 rows into mart_cost_optimization
  ✓ Completed in 23s

============================================================
  INVENTORY OPTIMIZATION — EOQ & SAFETY STOCK
============================================================

  Loading data...
  Loaded 548,500 product KPI rows, 500 products
  Running EOQ optimization...

  EOQ Optimization Summary:
    Products optimized: 500
    Avg EOQ           : 224.2 units
    Avg optimal SS    : 70.5 units
    Avg current SS    : 29.6 units

  ✓ Results saved to optimization/results/inventory_optimization_results.csv
  ✓ Completed in 10s

============================================================
  Optimization complete in 37s
============================================================


(base) (last-mile-fulfilment-optimization-py3.11) PS C:\Users\bhavy\OneDrive\Desktop\GITHUB\BHAVYA\Last-Mile-Fulfilment-Optimization>python -m ml.training.predict_and_writeback --phase future_demand
============================================================
  FULFILLMENT PLATFORM — PREDICTION & WRITEBACK
  Phases: future_demand
============================================================

============================================================
  FUTURE DEMAND FORECAST — PREDICT & INSERT
============================================================
  Model loaded: C:\Users\bhavy\OneDrive\Desktop\GITHUB\BHAVYA\Last-Mile-Fulfilment-Optimization\ml\saved_models\demand_best.joblib
  Model  : demand_best
  Horizon: 180 days

  Historical data ends : 2026-02-28
  Forecast window      : 2026-03-01 → 2026-08-27

  Pulling seed data (2025-12-30 → 2026-02-28) for lag features...
  Loaded 30,500 seed rows
  Loading product attributes from stg_products...
  Loaded 500 products

  Building future date spine...
  180 days × 500 products = 90,000 rows
  Extended dates dimension to 2026-08-27 (+180 rows)
  Building features for future rows...
  Scoring 90,000 future rows

  Sample future predictions:
      date product_id  demand_forecast  forecast_horizon
2026-03-01  PROD-0001             0.05                30
2026-03-02  PROD-0001             0.00                30
2026-03-03  PROD-0001             0.00                30
2026-03-04  PROD-0001             0.00                30
2026-03-05  PROD-0001             0.02                30
2026-03-06  PROD-0001             0.00                30
2026-03-07  PROD-0001             0.00                30
2026-03-08  PROD-0001             4.38                30
2026-03-09  PROD-0001             4.63                30
2026-03-10  PROD-0001             4.65                30

  Horizon distribution:
     30d bucket:   15,000 rows
     60d bucket:   15,000 rows
     90d bucket:   15,000 rows
    180d bucket:   45,000 rows

  Writing forecast vintage: 2026-02-28

  ✓ Merged 90,000 future forecast rows (vintage: 2026-02-28)
  ✓ Completed in 14s

============================================================
  All predictions complete in 14s
============================================================
(base) (last-mile-fulfilment-optimization-py3.11) PS C:\Users\bhavy\OneDrive\Desktop\GITHUB\BHAVYA\Last-Mile-Fulfilment-Optimization>python -m optimization.run_optimization
============================================================
  FULFILLMENT PLATFORM — OPTIMIZATION ENGINE
  Mode: full
============================================================

============================================================
  COST OPTIMIZATION — BASELINE VS OPTIMIZED
============================================================

  Loading mart_daily_warehouse_kpis...
  Loaded 11,912 rows
  Computing baseline costs...
  Applying optimization model...
  Computing allocation efficiency...

  Cost optimization summary:
    Total baseline cost : $3,629,183,554.38
    Total optimized cost: $3,331,586,272.65
    Total savings       : $ 297,597,281.73
    Avg savings pct     :           8.21%
    Avg allocation eff  :          81.65%

  ✓ Merged 11,912 rows into mart_cost_optimization
  ✓ Completed in 39s

============================================================
  INVENTORY OPTIMIZATION — EOQ & SAFETY STOCK
============================================================

  Loading data...
  Loaded 744,500 product KPI rows, 500 products
  Running EOQ optimization...

  EOQ Optimization Summary:
    Products optimized: 500
    Avg EOQ           : 223.1 units
    Avg optimal SS    : 70.5 units
    Avg current SS    : 29.6 units

  ✓ Results saved to optimization/results/inventory_optimization_results.csv
  ✓ Completed in 8s

============================================================
  Optimization complete in 50s
============================================================
(base) (last-mile-fulfilment-optimization-py3.11) PS C:\Users\bhavy\OneDrive\Desktop\GITHUB\BHAVYA\Last-Mile-Fulfilment-Optimization>python -m experimentation.run_experimentation
============================================================
  FULFILLMENT PLATFORM — EXPERIMENTATION ENGINE
  Mode: full
============================================================

  Loading experiment data from Snowflake...
  Loading dim_experiments...
    10 experiments
  Loading fact_experiment_assignments...
    1,711,415 assignments
  Loading fact_orders...
    7,178,484 orders
  Loading fact_deliveries...
    6,217,495 deliveries

  Preparing observations with treatment effects...
  Preparing EXP-001: Inventory Policy: Dynamic vs Static Reorder (Completed)
    86,845 observations — Control: 43,168, Treatment: 43,677
  Preparing EXP-002: Inventory Policy: Safety Stock +20% (Completed)
    99,048 observations — Control: 49,434, Treatment: 49,614
  Preparing EXP-003: Routing: Greedy vs Balanced (Completed)
    61,644 observations — Control: 30,604, Treatment: 31,040
  Preparing EXP-004: Routing: Nearest Driver vs Load-Balanced (Completed)
    74,294 observations — Control: 37,048, Treatment: 37,246
  Preparing EXP-005: Allocation: Nearest vs Cost-Optimal Warehouse (Completed)
    172,029 observations — Control: 85,810, Treatment: 86,219
  Preparing EXP-006: Allocation: Capacity-Aware Assignment (Completed)
    107,439 observations — Control: 53,677, Treatment: 53,762
  Preparing EXP-007: Inventory Policy: Just-In-Time Reorder (Completed)
    72,094 observations — Control: 36,233, Treatment: 35,861
  Preparing EXP-008: Routing: Priority-Based Driver Assignment (Completed)
    73,094 observations — Control: 36,561, Treatment: 36,533
  Preparing EXP-009: Allocation: Region-Locked vs Flexible (Active)
    340,567 observations — Control: 170,376, Treatment: 170,191
  Preparing EXP-010: Inventory Policy: ML-Driven Reorder Points (Active)
    505,471 observations — Control: 252,398, Treatment: 253,073
  Ready: 10 experiments prepared

============================================================
  STATISTICAL TESTS — WELCH T-TEST
============================================================

  Running Welch t-tests for all experiments...
  Testing EXP-001...
    p=0.0000 | SIGNIFICANT | lift=-10.45%
  Testing EXP-002...
    p=0.0000 | SIGNIFICANT | lift=-9.88%
  Testing EXP-003...
    p=0.0000 | SIGNIFICANT | lift=-11.06%
  Testing EXP-004...
    p=0.0000 | SIGNIFICANT | lift=-11.46%
  Testing EXP-005...
    p=0.0000 | SIGNIFICANT | lift=-8.24%
  Testing EXP-006...
    p=0.0000 | SIGNIFICANT | lift=-8.09%
  Testing EXP-007...
    p=0.0000 | SIGNIFICANT | lift=-11.92%
  Testing EXP-008...
    p=0.0000 | SIGNIFICANT | lift=-11.64%
  Testing EXP-009...
    p=0.0093 | SIGNIFICANT | lift=-1.58%
  Testing EXP-010...
    p=0.0000 | SIGNIFICANT | lift=-3.48%

======================================================================
  A/B TEST RESULTS SUMMARY
======================================================================
  ID         Type                      p-value    Lift %     Significant
  ---------- ------------------------- ---------- ---------- -----------
  EXP-001    inventory_policy          0.0000     -10.45%    YES ***
  EXP-002    inventory_policy          0.0000     -9.88%     YES ***
  EXP-003    routing_algorithm         0.0000     -11.06%    YES ***
  EXP-004    routing_algorithm         0.0000     -11.46%    YES ***
  EXP-005    warehouse_allocation      0.0000     -8.24%     YES ***
  EXP-006    warehouse_allocation      0.0000     -8.09%     YES ***
  EXP-007    inventory_policy          0.0000     -11.92%    YES ***
  EXP-008    routing_algorithm         0.0000     -11.64%    YES ***
  EXP-009    warehouse_allocation      0.0093     -1.58%     YES ***
  EXP-010    inventory_policy          0.0000     -3.48%     YES ***

  10/10 experiments significant at alpha=0.05
======================================================================

  ✓ Merged 20 rows into mart_experiment_results
  ✓ Completed in 6s

============================================================
  UPLIFT ANALYSIS — SEGMENT BREAKDOWN
============================================================

  Loading warehouse dimension for region mapping...
  Running uplift analysis...
  Uplift analysis for EXP-001...
    customer_segment: 3 segments
    order_priority: 3 segments
    warehouse_region: 4 segments
    Saved to experimentation/results/uplift_EXP-001.csv
  Uplift analysis for EXP-002...
    customer_segment: 3 segments
    order_priority: 3 segments
    warehouse_region: 4 segments
    Saved to experimentation/results/uplift_EXP-002.csv
  Uplift analysis for EXP-003...
    customer_segment: 3 segments
    order_priority: 3 segments
    warehouse_region: 4 segments
    Saved to experimentation/results/uplift_EXP-003.csv
  Uplift analysis for EXP-004...
    customer_segment: 3 segments
    order_priority: 3 segments
    warehouse_region: 4 segments
    Saved to experimentation/results/uplift_EXP-004.csv
  Uplift analysis for EXP-005...
    customer_segment: 3 segments
    order_priority: 3 segments
    warehouse_region: 8 segments
    Saved to experimentation/results/uplift_EXP-005.csv
  Uplift analysis for EXP-006...
    customer_segment: 3 segments
    order_priority: 3 segments
    warehouse_region: 4 segments
    Saved to experimentation/results/uplift_EXP-006.csv
  Uplift analysis for EXP-007...
    customer_segment: 3 segments
    order_priority: 3 segments
    warehouse_region: 4 segments
    Saved to experimentation/results/uplift_EXP-007.csv
  Uplift analysis for EXP-008...
    customer_segment: 3 segments
    order_priority: 3 segments
    warehouse_region: 4 segments
    Saved to experimentation/results/uplift_EXP-008.csv
  Uplift analysis for EXP-009...
    customer_segment: 3 segments
    order_priority: 3 segments
    warehouse_region: 4 segments
    Saved to experimentation/results/uplift_EXP-009.csv
  Uplift analysis for EXP-010...
    customer_segment: 3 segments
    order_priority: 3 segments
    warehouse_region: 8 segments
    Saved to experimentation/results/uplift_EXP-010.csv

======================================================================
  UPLIFT HIGHLIGHTS — TOP SEGMENTS BY TREATMENT EFFECT
======================================================================
  EXP-007 | order_priority=Same-Day | 17.5% reduction | p=0.0000
  EXP-008 | region=Southeast | 16.3% reduction | p=0.0000
  EXP-004 | customer_segment=Premium | 15.5% reduction | p=0.0000
  EXP-004 | region=Southeast | 15.3% reduction | p=0.0000
  EXP-004 | order_priority=Express | 14.4% reduction | p=0.0000
  EXP-003 | region=Northeast | 14.4% reduction | p=0.0000
======================================================================

  ✓ Uplift CSVs saved to experimentation/results/
  ✓ Completed in 5s

============================================================
  Experimentation complete in 164s
============================================================
(base) (last-mile-fulfilment-optimization-py3.11) PS C:\Users\bhavy\OneDrive\Desktop\GITHUB\BHAVYA\Last-Mile-Fulfilment-Optimization> 