# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Silver: clean, typed, trusted
# MAGIC For each entity (parents before children), only Bronze rows newer than the entity's watermark:
# MAGIC 1. Parse STRING columns to typed columns (`try_` casts, so bad values become rule failures, not errors).
# MAGIC 2. Apply the named rules in `src/quality_rules.py`. Failures go to `silver.quarantine_<entity>`
# MAGIC    with a `failure_reason`, never silently dropped.
# MAGIC 3. De-duplicate on the business key (`ROW_NUMBER`, latest `_ingested_at` wins).
# MAGIC 4. `MERGE` into `silver.<entity>`: insert new keys, update only when the incoming row is newer.
# MAGIC 5. Reconcile: Bronze rows in = merged + quarantined + de-duplicated away.

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

from src.config import ENTITIES, FX_TARGET, LOAD_ORDER
from src.fx import PAYLOAD_SCHEMA
from src.helpers import (build_upsert_sql, fq_name, log_dq, log_run, merge_metrics, new_run_id,
                         safe_identifier, utc_now)
from src.transforms import (add_parent_order_flag, build_rules, parse_entity, select_clean,
                            select_quarantine)
from src.quality_rules import dedupe_latest, split_valid_quarantine

catalog = safe_identifier(dbutils.widgets.get("catalog"))
run_id = dbutils.widgets.get("run_id") or new_run_id()
run_log = fq_name(catalog, "ops", "pipeline_run_log")
dq_table = fq_name(catalog, "ops", "dq_results")
watermarks = fq_name(catalog, "ops", "silver_watermarks")
print(f"run_id={run_id}")

# COMMAND ----------


def get_watermark(entity):
    row = spark.table(watermarks).where(F.col("entity") == entity).select("last_ingested_at").first()
    return row[0] if row else None


def set_watermark(entity, value):
    spark.sql(f"""
        MERGE INTO {watermarks} t
        USING (SELECT '{entity}' AS entity, TIMESTAMP'{value}' AS last_ingested_at) s
        ON t.entity = s.entity
        WHEN MATCHED THEN UPDATE SET t.last_ingested_at = s.last_ingested_at, t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (entity, last_ingested_at, updated_at)
          VALUES (s.entity, s.last_ingested_at, current_timestamp())""")


def add_category_english(df):
    """Silver products carry the English category; untranslated categories are flagged, not dropped."""
    translation = spark.table(fq_name(catalog, "silver", "category_translation")) \
        .select("product_category_name", "product_category_name_english")
    return (df.join(translation, "product_category_name", "left")
              .withColumn("category_translation_missing",
                          F.col("product_category_name").isNotNull()
                          & F.col("product_category_name_english").isNull())
              .withColumn("product_category_name_english",
                          F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown"))))


def dq(check, table, rows_checked, rows_failed, status, details, severity="info"):
    return dict(run_id=run_id, check_name=check, table_name=table, severity=severity,
                rows_checked=rows_checked, rows_failed=rows_failed, status=status,
                details=details, checked_at=utc_now())


# COMMAND ----------

for name in LOAD_ORDER:
    spec = ENTITIES[name]
    started = utc_now()
    bronze = fq_name(catalog, "bronze", name)
    target = fq_name(catalog, "silver", name)
    quarantine_table = fq_name(catalog, "silver", f"quarantine_{name}")
    if not spark.catalog.tableExists(bronze.replace("`", "")):
        print(f"{name}: no bronze table yet, skipped")
        continue

    watermark = get_watermark(name)
    increment = spark.table(bronze)
    if watermark is not None:
        increment = increment.where(F.col("_ingested_at") > F.lit(watermark))
    rows_in = increment.count()
    if rows_in == 0:
        print(f"{name}: nothing new since {watermark}")
        log_run(spark, run_log, run_id=run_id, task="silver", entity=name, rows_in=0, rows_out=0,
                rows_quarantined=0, rows_deduplicated=0, started_at=started, ended_at=utc_now(),
                status="SUCCESS", message=f"no new bronze rows since {watermark}")
        continue
    new_watermark = increment.agg(F.max("_ingested_at")).first()[0]

    parsed = parse_entity(increment, spec)
    if spec.needs_parent_order:
        parsed = add_parent_order_flag(parsed, spark.table(fq_name(catalog, "silver", "orders")))
    valid, quarantined = split_valid_quarantine(parsed, build_rules(spec))
    kept, dropped = dedupe_latest(valid, [spec.clean_name(k) for k in spec.keys])

    clean = select_clean(kept, spec)
    if name == "products":
        clean = add_category_english(clean)
    clean = clean.withColumn("_silver_updated_at", F.current_timestamp())

    if not spark.catalog.tableExists(target.replace("`", "")):
        clean.limit(0).write.saveAsTable(target)
    clean.createOrReplaceTempView("silver_source")
    metrics = merge_metrics(spark.sql(build_upsert_sql(target, "silver_source", spec.keys)).collect())

    q = select_quarantine(quarantined, spec, run_id)
    q.write.mode("append").option("mergeSchema", "true").saveAsTable(quarantine_table)

    n_out, n_quarantined, n_dropped = clean.count(), q.count(), dropped.count()
    balanced = rows_in == n_out + n_quarantined + n_dropped
    reasons = {r["failure_reason"]: r["count"] for r in q.groupBy("failure_reason").count().collect()}
    rescued = increment.where(F.col("_rescued_data").isNotNull()).count()

    checks = [
        dq("row_reconciliation", f"silver.{name}", rows_in, 0 if balanced else 1,
           "PASS" if balanced else "FAIL",
           f"bronze_in={rows_in} = merged={n_out} + quarantined={n_quarantined} + deduplicated={n_dropped}",
           severity="critical"),
        dq("duplicate_business_key", f"bronze.{name}", rows_in, n_dropped,
           "WARN" if n_dropped else "PASS", f"keys={list(spec.keys)}; kept latest _ingested_at"),
        dq("quarantined_rows", f"silver.quarantine_{name}", rows_in, n_quarantined,
           "WARN" if n_quarantined else "PASS", f"by reason: {reasons}"),
        dq("schema_drift_rescued_data", f"bronze.{name}", rows_in, rescued,
           "WARN" if rescued else "PASS", "rows with columns outside the contract (see _rescued_data)"),
    ]
    log_dq(spark, dq_table, checks)
    set_watermark(name, new_watermark)
    log_run(spark, run_log, run_id=run_id, task="silver", entity=name, rows_in=rows_in, rows_out=n_out,
            rows_quarantined=n_quarantined, rows_deduplicated=n_dropped, started_at=started,
            ended_at=utc_now(), status="SUCCESS" if balanced else "FAILED",
            message=f"quarantine reasons: {reasons}", **metrics)
    print(f"{name}: in={rows_in} merged={n_out} quarantined={n_quarantined} deduped={n_dropped} "
          f"inserted={metrics['rows_inserted']} updated={metrics['rows_updated']} balanced={balanced}")
    if not balanced:
        raise RuntimeError(f"Row reconciliation failed for {name}")

# COMMAND ----------

# MAGIC %md ## FX rates: parse the raw JSON (latest payload wins)

# COMMAND ----------

started = utc_now()
latest = (spark.table(fq_name(catalog, "bronze", "fx_rates_raw"))
          .orderBy(F.col("_ingested_at").desc()).limit(1))
fx = (latest
      .select(F.from_json("payload", PAYLOAD_SCHEMA).alias("p"), "source", "_ingested_at")
      .select(F.explode("p.rates").alias("day", "rates"), "source", "_ingested_at")
      .select(F.to_date("day").alias("rate_date"),
              F.col("rates").getItem(FX_TARGET).cast("decimal(12,6)").alias("brl_usd_rate"),
              F.col("source").alias("_source"), "_ingested_at"))
fx.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq_name(catalog, "silver", "fx_rates"))
n_fx = spark.table(fq_name(catalog, "silver", "fx_rates")).count()
log_run(spark, run_log, run_id=run_id, task="silver", entity="fx_rates", rows_out=n_fx,
        started_at=started, ended_at=utc_now(), status="SUCCESS", message="published ECB days only; gaps filled in Gold")
print(f"fx_rates: {n_fx} published days")

# COMMAND ----------

display(spark.table(run_log).where((F.col("run_id") == run_id) & (F.col("task") == "silver")).orderBy("started_at"))
