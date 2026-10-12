# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup: schemas, Volumes, ops tables
# MAGIC Idempotent: safe to re-run. Creates `bronze`, `silver`, `gold`, `ops` in the chosen catalog,
# MAGIC the landing Volume `bronze.landing` (raw uploads, incoming batches, Auto Loader checkpoints),
# MAGIC the `gold.exports` Volume (CSV fallback for Power BI) and the `ops` logging tables.
# MAGIC
# MAGIC **After running:** upload the Olist CSVs to the path printed at the bottom.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "workspace", "Catalog")

# Make `src/` importable (the repo root is one level above notebooks/).
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + os.path.dirname(os.path.dirname(nb_path))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import EXPORTS_VOLUME, LANDING_VOLUME, volume_paths
from src.helpers import (DQ_RESULTS_DDL, LANDING_SUBDIRS, LAYER_SCHEMAS, PIPELINE_RUN_LOG_DDL,
                         WATERMARK_DDL, fq_name, safe_identifier, volume_path)

catalog = safe_identifier(dbutils.widgets.get("catalog"))

# COMMAND ----------

# The catalog must already exist (Free Edition may not allow CREATE CATALOG, so we don't try).
existing = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
if catalog not in existing:
    raise ValueError(f"Catalog {catalog!r} not found. Available: {existing}. Set the 'catalog' widget.")

comments = {
    "bronze": "Raw source data, all columns STRING, plus ingestion metadata.",
    "silver": "Cleaned, typed, de-duplicated data and per-entity quarantine tables.",
    "gold": "Star schema for reporting (facts with explicit grain, dimensions, views).",
    "ops": "Pipeline run log, data-quality results and watermarks.",
}
for schema in LAYER_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq_name(catalog, schema)} COMMENT '{comments[schema]}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {fq_name(catalog, 'bronze', LANDING_VOLUME)}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {fq_name(catalog, 'gold', EXPORTS_VOLUME)}")
for sub in LANDING_SUBDIRS:
    dbutils.fs.mkdirs(volume_path(catalog, "bronze", LANDING_VOLUME, sub))

spark.sql(PIPELINE_RUN_LOG_DDL.format(table=fq_name(catalog, "ops", "pipeline_run_log")))
spark.sql(DQ_RESULTS_DDL.format(table=fq_name(catalog, "ops", "dq_results")))
spark.sql(WATERMARK_DDL.format(table=fq_name(catalog, "ops", "silver_watermarks")))

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN `{catalog}`"))
paths = volume_paths(catalog)
print(f"Upload the Olist CSVs to : {paths['raw']}/")
print(f"Incoming batches go to   : {paths['incoming']}/<entity>/")
print(f"Auto Loader checkpoints  : {paths['checkpoints']}/")
print(f"Gold CSV exports         : {paths['exports']}/")
