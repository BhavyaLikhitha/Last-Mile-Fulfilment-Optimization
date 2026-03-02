SELECT MAX(order_date) FROM FULFILLMENT_DB.RAW.FACT_ORDERS;
-- Should advance by 1 day each daily run

SELECT MAX(date) FROM FULFILLMENT_DB.MARTS.MART_DAILY_WAREHOUSE_KPIS;
-- Should match fact_orders max date after dbt runs

SELECT MAX(date) FROM FULFILLMENT_DB.MARTS.MART_DAILY_PRODUCT_KPIS
WHERE is_forecast = FALSE;