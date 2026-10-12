# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Data quality and reconciliation
# MAGIC Every check writes one row to `ops.dq_results`. **Critical** checks fail the Job (so a bad
# MAGIC Gold build is visible in run history and triggers the failure e-mail); **warn** checks are
# MAGIC findings that are reported, not forced to pass.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (set by the Job)")
# Failure drill: "true" treats warnings (real Olist findings) as failures, so the Job goes red.
dbutils.widgets.dropdown("fail_on_warn", "false", ["false", "true"], "Fail on warnings (drill)")
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + os.path.dirname(os.path.dirname(nb_path))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from pyspark.sql import functions as F

from src.config import LOAD_ORDER
from src.helpers import fq_name, log_dq, log_run, new_run_id, safe_identifier, utc_now

catalog = safe_identifier(dbutils.widgets.get("catalog"))
run_id = dbutils.widgets.get("run_id") or new_run_id()
started = utc_now()
s = lambda n: fq_name(catalog, "silver", n)  # noqa: E731
g = lambda n: fq_name(catalog, "gold", n)    # noqa: E731

# COMMAND ----------

# (check_name, table, severity, SQL returning rows_checked and rows_failed, details)
CHECKS = [
    ("fact_items_count_matches_silver", "gold.fact_order_items", "critical",
     f"SELECT (SELECT count(*) FROM {s('order_items')}) AS rows_checked, "
     f"abs((SELECT count(*) FROM {s('order_items')}) - (SELECT count(*) FROM {g('fact_order_items')})) AS rows_failed",
     "fact row count must equal Silver (no multiplied or lost rows)"),
    ("fact_items_revenue_matches_silver", "gold.fact_order_items", "critical",
     f"SELECT 1 AS rows_checked, CASE WHEN (SELECT sum(price + freight_value) FROM {s('order_items')}) "
     f"= (SELECT sum(item_total_brl) FROM {g('fact_order_items')}) THEN 0 ELSE 1 END AS rows_failed",
     "sum(price + freight) in Gold equals Silver"),
    ("fact_items_grain_unique", "gold.fact_order_items", "critical",
     f"SELECT count(*) AS rows_checked, count(*) - count(DISTINCT order_id, order_item_id) AS rows_failed "
     f"FROM {g('fact_order_items')}", "one row per (order_id, order_item_id)"),
    ("fact_payments_count_matches_silver", "gold.fact_order_payments", "critical",
     f"SELECT (SELECT count(*) FROM {s('order_payments')}) AS rows_checked, "
     f"abs((SELECT count(*) FROM {s('order_payments')}) - (SELECT count(*) FROM {g('fact_order_payments')})) AS rows_failed",
     "fact row count must equal Silver"),
    ("fact_payments_value_matches_silver", "gold.fact_order_payments", "critical",
     f"SELECT 1 AS rows_checked, CASE WHEN (SELECT sum(payment_value) FROM {s('order_payments')}) "
     f"= (SELECT sum(payment_value) FROM {g('fact_order_payments')}) THEN 0 ELSE 1 END AS rows_failed",
     "sum(payment_value) in Gold equals Silver"),
    ("fact_items_null_fx_rate", "gold.fact_order_items", "critical",
     f"SELECT count(*) AS rows_checked, count_if(brl_usd_rate IS NULL) AS rows_failed FROM {g('fact_order_items')}",
     "every item has the FX rate for its order date"),
    ("fact_payments_null_fx_rate", "gold.fact_order_payments", "critical",
     f"SELECT count(*) AS rows_checked, count_if(brl_usd_rate IS NULL) AS rows_failed FROM {g('fact_order_payments')}",
     "every payment has the FX rate for its order date"),
    ("fx_rate_positive", "gold.fx_rate_daily", "critical",
     f"SELECT count(*) AS rows_checked, count_if(brl_usd_rate IS NULL OR brl_usd_rate <= 0) AS rows_failed "
     f"FROM {g('fx_rate_daily')}", "no gaps or non-positive rates after forward-fill"),
    ("dim_customer_one_current_version", "gold.dim_customer", "critical",
     f"SELECT count(*) AS rows_checked, count_if(n_current <> 1) AS rows_failed FROM ("
     f"SELECT customer_unique_id, count_if(is_current) AS n_current FROM {g('dim_customer')} "
     f"GROUP BY customer_unique_id)", "SCD2: exactly one current row per customer"),
    ("fact_items_missing_customer", "gold.fact_order_items", "warn",
     f"SELECT count(*) AS rows_checked, count_if(customer_unique_id IS NULL) AS rows_failed FROM {g('fact_order_items')}",
     "order's customer row not (yet) in Silver"),
    ("delivered_before_purchase", "silver.orders", "warn",
     f"SELECT count(*) AS rows_checked, count_if(order_delivered_customer_date < order_purchase_timestamp) "
     f"AS rows_failed FROM {s('orders')}", "flag only; rows are kept"),
    ("delivered_without_delivery_date", "silver.orders", "warn",
     f"SELECT count(*) AS rows_checked, count_if(order_status = 'delivered' AND order_delivered_customer_date IS NULL) "
     f"AS rows_failed FROM {s('orders')}", "flag only; rows are kept"),
    ("orders_without_items", "gold.v_orders", "warn",
     f"SELECT count(*) AS rows_checked, count_if(item_count = 0) AS rows_failed FROM {g('v_orders')}",
     "orders with no item lines (mostly canceled/unavailable)"),
    ("payments_vs_items_mismatch", "gold.v_orders", "warn",
     f"SELECT count_if(item_count > 0 AND payment_count > 0) AS rows_checked, "
     f"count_if(item_count > 0 AND payment_count > 0 AND payment_mismatch) AS rows_failed FROM {g('v_orders')}",
     "sum(price + freight) <> sum(payment_value) by more than 0.01; reported, not forced to match"),
    ("untranslated_categories", "gold.dim_product", "warn",
     f"SELECT count(*) AS rows_checked, count_if(category_translation_missing) AS rows_failed FROM {g('dim_product')}",
     "products whose category has no English translation"),
]

results, failures = [], []
for name, table, severity, sql_text, details in CHECKS:
    try:
        row = spark.sql(sql_text).first()
        checked, failed = int(row["rows_checked"] or 0), int(row["rows_failed"] or 0)
        status = "PASS" if failed == 0 else ("FAIL" if severity == "critical" else "WARN")
    except Exception as e:
        checked, failed, status = None, None, "ERROR"
        details = f"{details} | {type(e).__name__}: {str(e)[:300]}"
    drill = status == "WARN" and dbutils.widgets.get("fail_on_warn") == "true"
    if (status in ("FAIL", "ERROR") and severity == "critical") or drill:
        failures.append(name)
    results.append(dict(run_id=run_id, check_name=name, table_name=table, severity=severity,
                        rows_checked=checked, rows_failed=failed, status=status, details=details,
                        checked_at=utc_now()))

# COMMAND ----------

# MAGIC %md ## Quarantine totals (all runs so far)

# COMMAND ----------

for entity in LOAD_ORDER:
    q = s(f"quarantine_{entity}")
    if spark.catalog.tableExists(q.replace("`", "")):
        by_reason = {r[0]: r[1] for r in spark.table(q).groupBy("failure_reason").count().collect()}
        total = sum(by_reason.values())
        results.append(dict(run_id=run_id, check_name="quarantine_total", table_name=f"silver.quarantine_{entity}",
                            severity="info", rows_checked=None, rows_failed=total,
                            status="INFO", details=f"by reason: {by_reason}", checked_at=utc_now()))

log_dq(spark, fq_name(catalog, "ops", "dq_results"), results)
log_run(spark, fq_name(catalog, "ops", "pipeline_run_log"), run_id=run_id, task="dq_checks",
        started_at=started, ended_at=utc_now(), status="FAILED" if failures else "SUCCESS",
        message=f"critical failures: {failures}" if failures else f"{len(results)} checks written")
display(spark.table(fq_name(catalog, "ops", "dq_results")).where(F.col("run_id") == run_id)
        .orderBy(F.col("severity"), F.col("status").desc()))

# COMMAND ----------

if failures:
    raise RuntimeError(f"Data-quality checks failed: {failures}. See ops.dq_results for run {run_id}.")
