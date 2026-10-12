# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Table maintenance and performance evidence
# MAGIC * **Small files:** Bronze grows by one small file per monthly batch. `OPTIMIZE` compacts them;
# MAGIC   file counts before/after are logged.
# MAGIC * **No partitioning:** ~100k orders is far below the size where partitions help (rule of thumb:
# MAGIC   partitions of ~1 GB+, tables of 1 TB+). Facts use **liquid clustering** on `order_date_key` instead.
# MAGIC * **History / time travel:** `DESCRIBE HISTORY` and `VERSION AS OF` on `silver.orders`.
# MAGIC * **EXPLAIN + timing** for one Gold query. Numbers are whatever this run measured.
# MAGIC * `VACUUM` keeps the default 7-day retention so time travel keeps working.

# COMMAND ----------

import os
import sys
import time

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (set by the Job)")
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + os.path.dirname(os.path.dirname(nb_path))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import LOAD_ORDER
from src.helpers import fq_name, log_run, new_run_id, safe_identifier, utc_now

catalog = safe_identifier(dbutils.widgets.get("catalog"))
run_id = dbutils.widgets.get("run_id") or new_run_id()
run_log = fq_name(catalog, "ops", "pipeline_run_log")

# COMMAND ----------

tables = [("bronze", e) for e in LOAD_ORDER] + [("silver", e) for e in LOAD_ORDER] + \
         [("gold", t) for t in ("fact_order_items", "fact_order_payments", "dim_customer")]
rows = []
for schema, name in tables:
    t = fq_name(catalog, schema, name)
    if not spark.catalog.tableExists(t.replace("`", "")):
        continue
    started = utc_now()
    before = spark.sql(f"DESCRIBE DETAIL {t}").first()["numFiles"]
    spark.sql(f"OPTIMIZE {t}")
    after = spark.sql(f"DESCRIBE DETAIL {t}").first()["numFiles"]
    rows.append((f"{schema}.{name}", before, after))
    log_run(spark, run_log, run_id=run_id, task="maintenance", entity=f"{schema}.{name}",
            started_at=started, ended_at=utc_now(), status="SUCCESS",
            message=f"OPTIMIZE numFiles {before} -> {after}")
display(spark.createDataFrame(rows, "table string, files_before long, files_after long"))

# COMMAND ----------

# MAGIC %md ## Delta history and time travel

# COMMAND ----------

orders = fq_name(catalog, "silver", "orders")
history = spark.sql(f"DESCRIBE HISTORY {orders}").select("version", "timestamp", "operation", "operationMetrics")
display(history)
first_version = history.agg({"version": "min"}).first()[0]
then = spark.sql(f"SELECT count(*) AS n FROM {orders} VERSION AS OF {first_version}").first()["n"]
now = spark.table(orders).count()
print(f"silver.orders rows at version {first_version}: {then}; now: {now}")

# COMMAND ----------

# MAGIC %md ## EXPLAIN and timing (monthly USD revenue by category)

# COMMAND ----------

query = f"""
SELECT d.year_month, p.category, sum(f.item_total_usd) AS revenue_usd
FROM {fq_name(catalog, 'gold', 'fact_order_items')} f
JOIN {fq_name(catalog, 'gold', 'dim_date')} d ON d.date_key = f.order_date_key
JOIN {fq_name(catalog, 'gold', 'dim_product')} p ON p.product_id = f.product_id
WHERE d.year = 2018
GROUP BY d.year_month, p.category"""
print(spark.sql(f"EXPLAIN FORMATTED {query}").first()[0])
t0 = time.time()
n = spark.sql(query).count()
print(f"{n} result rows in {time.time() - t0:.2f}s (this run, serverless; includes planning)")
