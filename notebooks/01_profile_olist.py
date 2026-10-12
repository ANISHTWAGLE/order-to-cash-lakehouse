# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Profile the raw Olist files (read-only)
# MAGIC Row counts, nulls per column, duplicate keys, date ranges, status values, orders without
# MAGIC items, and payments vs item totals. The findings decide which data-quality rules matter.
# MAGIC Nothing is written.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "workspace", "Catalog")
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + os.path.dirname(os.path.dirname(nb_path))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from pyspark.sql import functions as F

from src.config import ENTITIES, LOAD_ORDER, volume_paths
from src.helpers import safe_identifier

catalog = safe_identifier(dbutils.widgets.get("catalog"))
raw = volume_paths(catalog)["raw"]


def read_raw(entity):
    path = f"{raw}/{ENTITIES[entity].source_file}"
    return spark.read.option("header", "true").option("encoding", "UTF-8").csv(path)


frames = {e: read_raw(e) for e in LOAD_ORDER}

# COMMAND ----------

# MAGIC %md ## Row counts, duplicate keys and nulls per column

# COMMAND ----------

rows = []
for name, df in frames.items():
    spec = ENTITIES[name]
    keys = [c.lstrip("﻿") for c in spec.keys]
    df = df.toDF(*[c.lstrip("﻿") for c in df.columns])  # strip a UTF-8 BOM if present
    frames[name] = df
    total = df.count()
    distinct_keys = df.select(*keys).distinct().count()
    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first().asDict()
    rows.append((name, total, total - distinct_keys, {k: v for k, v in nulls.items() if v}))

display(spark.createDataFrame(rows, "entity string, row_count long, duplicate_keys long, nulls_per_column map<string,long>"))

# COMMAND ----------

# MAGIC %md ## Orders: status values and date ranges

# COMMAND ----------

orders, items, payments = frames["orders"], frames["order_items"], frames["order_payments"]
display(orders.groupBy("order_status").count().orderBy(F.desc("count")))
display(orders.select(
    F.min("order_purchase_timestamp").alias("first_purchase"),
    F.max("order_purchase_timestamp").alias("last_purchase"),
    F.sum(((F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNull()).cast("int"))
     .alias("delivered_without_date"),
    F.sum((F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp")).cast("int"))
     .alias("delivered_before_purchase"),
))
display(orders.groupBy(F.substring("order_purchase_timestamp", 1, 7).alias("purchase_month")).count().orderBy("purchase_month"))

# COMMAND ----------

# MAGIC %md ## Relationships and money

# COMMAND ----------

item_totals = items.groupBy("order_id").agg(F.sum(F.col("price") + F.col("freight_value")).alias("items_total"))
pay_totals = payments.groupBy("order_id").agg(F.sum("payment_value").alias("payments_total"))
both = item_totals.join(pay_totals, "order_id")
display(spark.createDataFrame([(
    orders.join(items, "order_id", "left_anti").count(),
    items.join(orders, "order_id", "left_anti").count(),
    payments.join(orders, "order_id", "left_anti").count(),
    both.where(F.abs(F.col("items_total") - F.col("payments_total")) > 0.01).count(),
    both.count(),
)], "orders_without_items long, orphan_items long, orphan_payments long, "
    "orders_items_vs_payments_mismatch long, orders_with_items_and_payments long"))

display(payments.groupBy("payment_type").agg(
    F.count("*").alias("rows"),
    F.sum((F.col("payment_value").cast("double") == 0).cast("int")).alias("zero_value"),
    F.sum((F.col("payment_installments").cast("int") == 0).cast("int")).alias("zero_installments")))

# COMMAND ----------

# MAGIC %md ## Product categories without an English translation

# COMMAND ----------

display(frames["products"].select("product_category_name").distinct()
        .join(frames["category_translation"], "product_category_name", "left_anti")
        .where(F.col("product_category_name").isNotNull()))
