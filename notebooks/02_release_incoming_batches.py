# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Release incoming batches (simulates the source system)
# MAGIC Olist arrives as one file per table. To test incremental loading, this notebook splits
# MAGIC orders, items, payments and customers **by order purchase month** and drops the monthly
# MAGIC files into `incoming/<entity>/`, where Bronze picks up only files it has not seen.
# MAGIC
# MAGIC | `release` | What lands in `incoming/` |
# MAGIC |---|---|
# MAGIC | `initial` | reference tables + months up to 2017-12 |
# MAGIC | `remaining` | months from 2018-01 |
# MAGIC | `all` | reference tables + every month |
# MAGIC | `simulated_correction` | **simulated** status corrections + customer moves (`data/simulated`) |
# MAGIC | `simulated_faults` | **simulated** fault files (`data/simulated`) |
# MAGIC
# MAGIC Files that already exist are never rewritten, so re-running is safe.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("release", "initial",
                         ["initial", "remaining", "all", "simulated_correction", "simulated_faults"], "Release")
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + os.path.dirname(os.path.dirname(nb_path))
if repo_root not in sys.path:
    sys.path.append(repo_root)

import glob
import shutil

import pandas as pd

from src.config import ENTITIES, LOAD_ORDER, volume_paths
from src.helpers import safe_identifier

catalog = safe_identifier(dbutils.widgets.get("catalog"))
release = dbutils.widgets.get("release")
paths = volume_paths(catalog)
CUTOVER = "2018-01"

# COMMAND ----------

written, skipped = [], []


def put(df: pd.DataFrame, entity: str, file_name: str):
    folder = f"{paths['incoming']}/{entity}"
    os.makedirs(folder, exist_ok=True)
    target = f"{folder}/{file_name}"
    if os.path.exists(target):
        skipped.append(target)
        return
    df.to_csv(target, index=False)
    written.append((target, len(df)))


def read_raw(entity):
    # dtype=str + keep_default_na=False: keep every value exactly as the source wrote it.
    # utf-8-sig strips the BOM that product_category_name_translation.csv starts with.
    return pd.read_csv(f"{paths['raw']}/{ENTITIES[entity].source_file}",
                       dtype=str, keep_default_na=False, encoding="utf-8-sig")


def release_olist(month_filter):
    orders = read_raw("orders")
    orders["_month"] = orders["order_purchase_timestamp"].str[:7]
    order_month = orders.set_index("order_id")["_month"]
    customer_month = orders.set_index("customer_id")["_month"]
    for entity in LOAD_ORDER:
        spec = ENTITIES[entity]
        if not spec.split_by_order_month:
            if month_filter("0000-00"):  # reference tables ship with the initial load
                put(read_raw(entity), entity, spec.source_file)
            continue
        df = orders if entity == "orders" else read_raw(entity)
        if entity == "customers":
            months = df["customer_id"].map(customer_month)
        elif entity != "orders":
            months = df["order_id"].map(order_month)
        else:
            months = df["_month"]
        for month in sorted(months.dropna().unique()):
            if month_filter(month):
                part = df[months == month][list(spec.columns)]
                put(part, entity, f"olist_{entity}_{month.replace('-', '_')}.csv")


def release_simulated(prefix):
    for path in sorted(glob.glob(f"{repo_root}/data/simulated/*/{prefix}*.csv")):
        entity = os.path.basename(os.path.dirname(path))
        target_dir = f"{paths['incoming']}/{entity}"
        os.makedirs(target_dir, exist_ok=True)
        target = f"{target_dir}/{os.path.basename(path)}"
        if os.path.exists(target):
            skipped.append(target)
        else:
            shutil.copyfile(path, target)
            written.append((target, sum(1 for _ in open(path)) - 1))


if release == "initial":
    release_olist(lambda m: m < CUTOVER)
elif release == "remaining":
    release_olist(lambda m: m >= CUTOVER and m != "0000-00")
elif release == "all":
    release_olist(lambda m: True)
elif release == "simulated_correction":
    release_simulated("simulated_correction")
elif release == "simulated_faults":
    release_simulated("simulated_fault")

# COMMAND ----------

print(f"release={release}: {len(written)} files written, {len(skipped)} already present (left untouched)")
if written:
    display(spark.createDataFrame(written, "file string, data_rows long"))
