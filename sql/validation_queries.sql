-- Validation queries. Run in the Databricks SQL editor after a pipeline run.
-- Change the catalog if you used a different one.
USE CATALOG workspace;

-- 1. Facts are not multiplied: counts and money match Silver exactly.
SELECT 'order_items' AS fact,
       (SELECT count(*) FROM silver.order_items)            AS silver_rows,
       (SELECT count(*) FROM gold.fact_order_items)         AS gold_rows,
       (SELECT sum(price + freight_value) FROM silver.order_items) AS silver_brl,
       (SELECT sum(item_total_brl) FROM gold.fact_order_items)     AS gold_brl
UNION ALL
SELECT 'order_payments',
       (SELECT count(*) FROM silver.order_payments),
       (SELECT count(*) FROM gold.fact_order_payments),
       (SELECT sum(payment_value) FROM silver.order_payments),
       (SELECT sum(payment_value) FROM gold.fact_order_payments);

-- 2. Why items and payments must never be joined directly: the naive join inflates revenue.
SELECT 'naive items x payments join' AS method, sum(i.price + i.freight_value) AS revenue_brl
FROM silver.order_items i JOIN silver.order_payments p ON p.order_id = i.order_id
UNION ALL
SELECT 'fact_order_items (correct)', sum(item_total_brl) FROM gold.fact_order_items;

-- 3. Grain check: no duplicate keys in either fact.
SELECT 'fact_order_items' AS fact, count(*) - count(DISTINCT order_id, order_item_id) AS duplicate_keys
FROM gold.fact_order_items
UNION ALL
SELECT 'fact_order_payments', count(*) - count(DISTINCT order_id, payment_sequential)
FROM gold.fact_order_payments;

-- 4. ROW_NUMBER: each customer's most recent order (dedup / latest-record pattern).
SELECT customer_unique_id, order_id, order_purchase_timestamp
FROM (
  SELECT customer_unique_id, order_id, order_purchase_timestamp,
         ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp DESC) AS rn
  FROM gold.v_orders)
WHERE rn = 1
LIMIT 20;

-- 5. LAG: month-over-month USD revenue change.
WITH monthly AS (
  SELECT d.year_month, sum(f.item_total_usd) AS revenue_usd
  FROM gold.fact_order_items f JOIN gold.dim_date d ON d.date_key = f.order_date_key
  GROUP BY d.year_month)
SELECT year_month, revenue_usd,
       LAG(revenue_usd) OVER (ORDER BY year_month) AS prev_month_usd,
       round(100 * (revenue_usd - LAG(revenue_usd) OVER (ORDER BY year_month))
             / NULLIF(LAG(revenue_usd) OVER (ORDER BY year_month), 0), 1) AS mom_change_pct
FROM monthly
ORDER BY year_month;

-- 6. NULL handling: delivery time only where both dates exist; count what is excluded.
SELECT order_status,
       count(*)                                   AS orders,
       count(delivery_days)                       AS orders_with_delivery_days,
       count(*) - count(delivery_days)            AS excluded_null_dates,
       round(avg(delivery_days), 1)               AS avg_delivery_days,
       round(100 * avg(CAST(is_late AS INT)), 1)  AS late_pct
FROM gold.v_orders
GROUP BY order_status
ORDER BY orders DESC;

-- 7. FX coverage: weekends/holidays filled, no gaps.
SELECT count(*) AS days, count_if(is_filled) AS filled_days, count_if(brl_usd_rate IS NULL) AS null_days,
       min(rate_date) AS first_day, max(rate_date) AS last_day
FROM gold.fx_rate_daily;

-- 8. SCD2: customers with more than one version (address changed between orders/loads).
SELECT customer_unique_id, customer_city, customer_state, valid_from, valid_to, is_current
FROM gold.dim_customer
WHERE customer_unique_id IN (
  SELECT customer_unique_id FROM gold.dim_customer GROUP BY customer_unique_id HAVING count(*) > 1)
ORDER BY customer_unique_id, valid_from
LIMIT 50;

-- 9. Quarantine overview.
SELECT 'orders' AS entity, failure_reason, count(*) AS rows FROM silver.quarantine_orders GROUP BY failure_reason
UNION ALL
SELECT 'order_items', failure_reason, count(*) FROM silver.quarantine_order_items GROUP BY failure_reason;

-- 10. Latest run: row flow per task and entity.
SELECT task, entity, rows_in, rows_out, rows_quarantined, rows_deduplicated, rows_inserted, rows_updated, status
FROM ops.pipeline_run_log
WHERE run_id = (SELECT run_id FROM ops.pipeline_run_log ORDER BY started_at DESC LIMIT 1)
ORDER BY started_at;
