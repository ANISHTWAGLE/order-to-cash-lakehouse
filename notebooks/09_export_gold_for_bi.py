# Databricks notebook source
# MAGIC %md
# MAGIC # 09 Export Gold to CSV (Power BI fallback)
# MAGIC Use this only if Power BI Desktop cannot connect to the workspace's SQL warehouse.
# MAGIC Writes one CSV per Gold table to the `gold.exports` Volume; download them from
# MAGIC Catalog Explorer and load them in Power BI (see `docs/power_bi.md`).

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "workspace", "Catalog")
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + os.path.dirname(os.path.dirname(nb_path))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import volume_paths
from src.helpers import fq_name, safe_identifier

catalog = safe_identifier(dbutils.widgets.get("catalog"))
out = volume_paths(catalog)["exports"]

for name in ("fact_order_items", "fact_order_payments", "v_dim_customer_current", "dim_product",
             "dim_seller", "dim_date", "v_orders"):
    pdf = spark.table(fq_name(catalog, "gold", name)).toPandas()
    pdf.to_csv(f"{out}/{name}.csv", index=False)
    print(f"{out}/{name}.csv  ({len(pdf)} rows)")
