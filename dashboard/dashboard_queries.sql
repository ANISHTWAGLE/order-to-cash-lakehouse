-- Queries behind the dashboard (Databricks AI/BI dashboard or Power BI).
USE CATALOG workspace;

-- KPI tiles
SELECT
  (SELECT sum(item_total_brl) FROM gold.fact_order_items)        AS revenue_brl,
  (SELECT sum(item_total_usd) FROM gold.fact_order_items)        AS revenue_usd,
  (SELECT count(*) FROM gold.v_orders)                           AS orders,
  (SELECT round(avg(delivery_days), 1) FROM gold.v_orders)       AS avg_delivery_days,
  (SELECT round(100 * avg(CAST(is_late AS INT)), 1) FROM gold.v_orders) AS late_delivery_pct;

-- Monthly revenue (USD)
SELECT d.year_month, sum(f.item_total_usd) AS revenue_usd, count(DISTINCT f.order_id) AS orders
FROM gold.fact_order_items f JOIN gold.dim_date d ON d.date_key = f.order_date_key
GROUP BY d.year_month ORDER BY d.year_month;

-- Top 10 categories by revenue
SELECT p.category, sum(f.item_total_brl) AS revenue_brl
FROM gold.fact_order_items f JOIN gold.dim_product p ON p.product_id = f.product_id
GROUP BY p.category ORDER BY revenue_brl DESC LIMIT 10;

-- Payment-type mix
SELECT payment_type, count(*) AS payments, sum(payment_value) AS value_brl,
       round(100 * sum(payment_value) / sum(sum(payment_value)) OVER (), 1) AS share_pct
FROM gold.fact_order_payments GROUP BY payment_type ORDER BY value_brl DESC;

-- Late deliveries by customer state
SELECT c.customer_state, count(*) AS delivered_orders, round(100 * avg(CAST(o.is_late AS INT)), 1) AS late_pct
FROM gold.v_orders o JOIN gold.v_dim_customer_current c ON c.customer_unique_id = o.customer_unique_id
WHERE o.order_status = 'delivered' AND o.delivery_days IS NOT NULL
GROUP BY c.customer_state ORDER BY delivered_orders DESC;
