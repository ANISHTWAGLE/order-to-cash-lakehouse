-- Order-level views. notebooks/05_gold_star_schema.py creates these on every run;
-- this file is the readable reference copy.
USE CATALOG workspace;

-- One row per order. Items and payments are aggregated separately BEFORE joining,
-- so an order with 2 items and 2 payments is still one row with correct totals.
CREATE OR REPLACE VIEW gold.v_orders AS
WITH items AS (
  SELECT order_id, count(*) AS item_count, sum(price + freight_value) AS items_total_brl
  FROM silver.order_items GROUP BY order_id),
pays AS (
  SELECT order_id, count(*) AS payment_count, sum(payment_value) AS payments_total_brl
  FROM silver.order_payments GROUP BY order_id)
SELECT o.order_id, c.customer_unique_id, o.order_status,
       CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_date_key,
       o.order_purchase_timestamp, o.order_delivered_customer_date, o.order_estimated_delivery_date,
       datediff(o.order_delivered_customer_date, o.order_purchase_timestamp) AS delivery_days,
       o.order_delivered_customer_date > o.order_estimated_delivery_date AS is_late,
       coalesce(i.item_count, 0) AS item_count, i.items_total_brl,
       coalesce(p.payment_count, 0) AS payment_count, p.payments_total_brl,
       abs(coalesce(i.items_total_brl, 0) - coalesce(p.payments_total_brl, 0)) > 0.01 AS payment_mismatch
FROM silver.orders o
LEFT JOIN silver.customers c ON c.customer_id = o.customer_id
LEFT JOIN items i ON i.order_id = o.order_id
LEFT JOIN pays p ON p.order_id = o.order_id;

-- Current customer version only (for BI relationships).
CREATE OR REPLACE VIEW gold.v_dim_customer_current AS
SELECT customer_sk, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state, valid_from
FROM gold.dim_customer
WHERE is_current;
