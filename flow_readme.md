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