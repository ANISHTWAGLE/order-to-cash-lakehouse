# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Gold: star schema
# MAGIC | Table | One row per |
# MAGIC |---|---|
# MAGIC | `fact_order_items` | order item (`order_id`, `order_item_id`) |
# MAGIC | `fact_order_payments` | payment record (`order_id`, `payment_sequential`) |
# MAGIC | `dim_customer` | customer version (SCD Type 2 on `customer_unique_id`) |
# MAGIC | `dim_product`, `dim_seller` | product, seller |
# MAGIC | `dim_date` | calendar date |
# MAGIC | `fx_rate_daily` | calendar date, BRL→USD forward-filled over weekends/holidays |
# MAGIC
# MAGIC **Items and payments are never joined into one table**: an order with 2 items and 2 payments
# MAGIC would become 4 rows and double the revenue. Order-level questions use the view `v_orders`.
# MAGIC
# MAGIC Facts and small dimensions are rebuilt from Silver each run (`CREATE OR REPLACE`), which is
# MAGIC idempotent and cheap at this size. `dim_customer` is maintained incrementally (SCD2).

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (set by the Job)")
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + os.path.dirname(os.path.dirname(nb_path))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from pyspark.sql import functions as F

from src.fx import forward_fill_df
from src.helpers import (build_scd2_close_sql, build_scd2_insert_sql, fq_name, log_run, new_run_id,
                         safe_identifier, utc_now)

catalog = safe_identifier(dbutils.widgets.get("catalog"))
run_id = dbutils.widgets.get("run_id") or new_run_id()
run_log = fq_name(catalog, "ops", "pipeline_run_log")


def s(name):
    return fq_name(catalog, "silver", name)


def g(name):
    return fq_name(catalog, "gold", name)


def build(name, sql_text):
    started = utc_now()
    spark.sql(sql_text)
    n = spark.table(g(name)).count()
    log_run(spark, run_log, run_id=run_id, task="gold", entity=name, rows_out=n,
            started_at=started, ended_at=utc_now(), status="SUCCESS")
    print(f"{name}: {n} rows")


# COMMAND ----------

# MAGIC %md ## FX: full calendar + forward-fill (rate for the order date, never today's)

# COMMAND ----------

bounds = spark.table(s("orders")).agg(F.min(F.to_date("order_purchase_timestamp")).alias("lo"),
                                      F.max(F.to_date("order_purchase_timestamp")).alias("hi")).first()
rates = spark.table(s("fx_rates")).select("rate_date", F.col("brl_usd_rate").cast("double").alias("rate"))
first_rate = rates.agg(F.min("rate_date")).first()[0]
start = min(first_rate, bounds["lo"])  # start at the first published rate so day 1 has a seed
filled = (forward_fill_df(spark, rates, str(start), str(bounds["hi"]))
          .select("rate_date", F.col("rate").cast("decimal(12,6)").alias("brl_usd_rate"), "is_filled"))
filled.createOrReplaceTempView("fx_filled")
build("fx_rate_daily", f"""
  CREATE OR REPLACE TABLE {g('fx_rate_daily')}
  COMMENT 'One row per calendar day. BRL->USD ECB reference rate; weekends/holidays carry the last published rate (is_filled = true).'
  AS SELECT * FROM fx_filled""")

# COMMAND ----------

# MAGIC %md ## Dimensions

# COMMAND ----------

build("dim_date", f"""
  CREATE OR REPLACE TABLE {g('dim_date')} COMMENT 'One row per calendar date over the order and delivery range.' AS
  WITH bounds AS (
    SELECT min(to_date(order_purchase_timestamp)) AS lo,
           greatest(max(to_date(order_estimated_delivery_date)), max(to_date(order_delivered_customer_date))) AS hi
    FROM {s('orders')})
  SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS date,
         year(d) AS year, quarter(d) AS quarter, month(d) AS month, date_format(d, 'MMMM') AS month_name,
         date_format(d, 'yyyy-MM') AS year_month, dayofweek(d) AS day_of_week,
         date_format(d, 'EEEE') AS day_name, dayofweek(d) IN (1, 7) AS is_weekend
  FROM bounds LATERAL VIEW explode(sequence(lo, hi, INTERVAL 1 DAY)) AS d""")

build("dim_product", f"""
  CREATE OR REPLACE TABLE {g('dim_product')} COMMENT 'One row per product. Category in English; untranslated categories keep the Portuguese name and are flagged.' AS
  SELECT product_id, product_category_name_english AS category, product_category_name AS category_pt,
         category_translation_missing, product_name_length, product_description_length,
         product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm
  FROM {s('products')}""")

build("dim_seller", f"""
  CREATE OR REPLACE TABLE {g('dim_seller')} COMMENT 'One row per seller.' AS
  SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state FROM {s('sellers')}""")

# COMMAND ----------

# MAGIC %md ## dim_customer (SCD Type 2)
# MAGIC In Olist, `customer_id` is new for **every order**; the real person is `customer_unique_id`.
# MAGIC The dimension is keyed on `customer_unique_id`; the tracked attributes are the address of the
# MAGIC person's latest order. A change closes the current version and opens a new one.

# COMMAND ----------

started = utc_now()
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {g('dim_customer')} (
    customer_sk BIGINT, customer_unique_id STRING, customer_zip_code_prefix STRING,
    customer_city STRING, customer_state STRING, attr_hash STRING,
    valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN)
  COMMENT 'One row per customer version (SCD2). Exactly one is_current row per customer_unique_id.'""")

spark.sql(f"""
  SELECT customer_unique_id, customer_zip_code_prefix, customer_city, customer_state,
         sha2(concat_ws('|', coalesce(customer_zip_code_prefix, ''), coalesce(customer_city, ''),
                        coalesce(customer_state, '')), 256) AS attr_hash
  FROM (
    SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.customer_unique_id
                                   ORDER BY o.order_purchase_timestamp DESC NULLS LAST, c._ingested_at DESC) AS rn
    FROM {s('customers')} c LEFT JOIN {s('orders')} o ON o.customer_id = c.customer_id)
  WHERE rn = 1""").createOrReplaceTempView("customer_latest")

ts = utc_now().strftime("%Y-%m-%d %H:%M:%S")
closed = spark.sql(build_scd2_close_sql(g("dim_customer"), "customer_latest", "customer_unique_id", ts)).collect()
spark.sql(build_scd2_insert_sql(g("dim_customer"), "customer_latest", "customer_unique_id",
                                ["customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"], ts))
versions = spark.sql(f"SELECT count(*) AS n, sum(CAST(is_current AS INT)) AS cur FROM {g('dim_customer')}").first()
n_closed = closed[0].asDict().get("num_updated_rows") if closed else None
log_run(spark, run_log, run_id=run_id, task="gold", entity="dim_customer", rows_out=versions["n"],
        rows_updated=n_closed, started_at=started, ended_at=utc_now(), status="SUCCESS",
        message=f"{versions['cur']} current customers, {versions['n']} versions, {n_closed} closed this run")
print(f"dim_customer: {versions['n']} versions, {versions['cur']} current, {n_closed} closed this run")

spark.sql(f"""CREATE OR REPLACE VIEW {g('v_dim_customer_current')}
  COMMENT 'Current version of each customer; use this for Power BI relationships.' AS
  SELECT customer_sk, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state, valid_from
  FROM {g('dim_customer')} WHERE is_current""")

# COMMAND ----------

# MAGIC %md ## Facts (liquid clustering on the date key)

# COMMAND ----------

build("fact_order_items", f"""
  CREATE OR REPLACE TABLE {g('fact_order_items')} CLUSTER BY (order_date_key)
  COMMENT 'Grain: one row per order item (order_id, order_item_id). Never joined to payments.' AS
  SELECT i.order_id, i.order_item_id, i.product_id, i.seller_id, c.customer_unique_id,
         CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_date_key,
         o.order_status, i.price, i.freight_value,
         CAST(i.price + i.freight_value AS DECIMAL(12,2)) AS item_total_brl,
         fx.brl_usd_rate,
         CAST(round(i.price * fx.brl_usd_rate, 2) AS DECIMAL(12,2)) AS price_usd,
         CAST(round(i.freight_value * fx.brl_usd_rate, 2) AS DECIMAL(12,2)) AS freight_usd,
         CAST(round((i.price + i.freight_value) * fx.brl_usd_rate, 2) AS DECIMAL(12,2)) AS item_total_usd
  FROM {s('order_items')} i
  JOIN {s('orders')} o ON o.order_id = i.order_id
  LEFT JOIN {s('customers')} c ON c.customer_id = o.customer_id
  LEFT JOIN {g('fx_rate_daily')} fx ON fx.rate_date = to_date(o.order_purchase_timestamp)""")

build("fact_order_payments", f"""
  CREATE OR REPLACE TABLE {g('fact_order_payments')} CLUSTER BY (order_date_key)
  COMMENT 'Grain: one row per payment record (order_id, payment_sequential). Never joined to items.' AS
  SELECT p.order_id, p.payment_sequential, p.payment_type, p.payment_installments, p.payment_value,
         c.customer_unique_id,
         CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_date_key,
         fx.brl_usd_rate,
         CAST(round(p.payment_value * fx.brl_usd_rate, 2) AS DECIMAL(12,2)) AS payment_value_usd
  FROM {s('order_payments')} p
  JOIN {s('orders')} o ON o.order_id = p.order_id
  LEFT JOIN {s('customers')} c ON c.customer_id = o.customer_id
  LEFT JOIN {g('fx_rate_daily')} fx ON fx.rate_date = to_date(o.order_purchase_timestamp)""")

# COMMAND ----------

# MAGIC %md ## Order-level view (counts, status, delivery time)

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE VIEW {g('v_orders')}
  COMMENT 'One row per order. Item and payment totals are aggregated separately before joining, so nothing is multiplied.' AS
  WITH items AS (
    SELECT order_id, count(*) AS item_count, sum(price + freight_value) AS items_total_brl
    FROM {s('order_items')} GROUP BY order_id),
  pays AS (
    SELECT order_id, count(*) AS payment_count, sum(payment_value) AS payments_total_brl
    FROM {s('order_payments')} GROUP BY order_id)
  SELECT o.order_id, c.customer_unique_id, o.order_status,
         CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_date_key,
         o.order_purchase_timestamp, o.order_delivered_customer_date, o.order_estimated_delivery_date,
         datediff(o.order_delivered_customer_date, o.order_purchase_timestamp) AS delivery_days,
         o.order_delivered_customer_date > o.order_estimated_delivery_date AS is_late,
         coalesce(i.item_count, 0) AS item_count, i.items_total_brl,
         coalesce(p.payment_count, 0) AS payment_count, p.payments_total_brl,
         abs(coalesce(i.items_total_brl, 0) - coalesce(p.payments_total_brl, 0)) > 0.01 AS payment_mismatch
  FROM {s('orders')} o
  LEFT JOIN {s('customers')} c ON c.customer_id = o.customer_id
  LEFT JOIN items i ON i.order_id = o.order_id
  LEFT JOIN pays p ON p.order_id = o.order_id""")

# COMMAND ----------

# MAGIC %md ## Governance: column comments and read access to Gold

# COMMAND ----------

column_comments = {
    "fact_order_items": {
        "order_date_key": "Purchase date as yyyymmdd; joins dim_date.date_key",
        "price": "Item price in BRL", "freight_value": "Freight in BRL",
        "brl_usd_rate": "ECB BRL->USD rate on the purchase date (forward-filled)",
        "customer_unique_id": "The person (Olist customer_id changes per order)",
    },
    "fact_order_payments": {
        "payment_value": "Payment in BRL; may differ from item totals (vouchers, instalment interest)",
        "brl_usd_rate": "ECB BRL->USD rate on the purchase date (forward-filled)",
    },
}
for table, cols in column_comments.items():
    for col, text in cols.items():
        spark.sql(f"ALTER TABLE {g(table)} ALTER COLUMN {col} COMMENT '{text}'")

# Free Edition may not have groups other than `account users`; failure here is reported, not fatal.
try:
    spark.sql(f"GRANT USE SCHEMA, SELECT ON SCHEMA {fq_name(catalog, 'gold')} TO `account users`")
    print("Granted USE SCHEMA + SELECT on gold to `account users`")
except Exception as e:
    print(f"GRANT skipped ({type(e).__name__}): {str(e)[:200]}")
