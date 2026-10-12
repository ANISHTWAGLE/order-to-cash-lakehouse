"""SQL builders and ops logging.

The MERGE/SCD2 builders are plain string functions so their idempotency
guards can be unit-tested without a Delta runtime.
"""
import re
import uuid
from datetime import datetime, timezone

LAYER_SCHEMAS = ("bronze", "silver", "gold", "ops")
LANDING_SUBDIRS = ("raw/olist", "incoming", "checkpoints")

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def safe_identifier(name) -> str:
    """Catalog/schema/table names come from widgets, so validate before putting them in SQL."""
    if not isinstance(name, str) or not _IDENT.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


def fq_name(*parts) -> str:
    return ".".join(f"`{safe_identifier(p)}`" for p in parts)


def volume_path(catalog: str, schema: str, volume: str, *subpaths) -> str:
    base = f"/Volumes/{safe_identifier(catalog)}/{safe_identifier(schema)}/{safe_identifier(volume)}"
    extra = "/".join(p.strip("/") for p in subpaths if p.strip("/"))
    return f"{base}/{extra}" if extra else base

PIPELINE_RUN_LOG_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  run_id STRING, task STRING, entity STRING,
  rows_in BIGINT, rows_out BIGINT, rows_quarantined BIGINT, rows_deduplicated BIGINT,
  rows_inserted BIGINT, rows_updated BIGINT,
  started_at TIMESTAMP, ended_at TIMESTAMP, status STRING, message STRING
) COMMENT 'One row per task (and entity) per pipeline run.'
"""

DQ_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  run_id STRING, check_name STRING, table_name STRING, severity STRING,
  rows_checked BIGINT, rows_failed BIGINT, status STRING, details STRING,
  checked_at TIMESTAMP
) COMMENT 'One row per data-quality check per pipeline run.'
"""

WATERMARK_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  entity STRING, last_ingested_at TIMESTAMP, updated_at TIMESTAMP
) COMMENT 'Highest Bronze _ingested_at already processed into Silver, per entity.'
"""

RUN_LOG_COLUMNS = (
    "run_id", "task", "entity", "rows_in", "rows_out", "rows_quarantined",
    "rows_deduplicated", "rows_inserted", "rows_updated",
    "started_at", "ended_at", "status", "message",
)
DQ_COLUMNS = (
    "run_id", "check_name", "table_name", "severity", "rows_checked",
    "rows_failed", "status", "details", "checked_at",
)


def new_run_id() -> str:
    return f"manual-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def build_upsert_sql(target: str, source_view: str, keys, sequence_col: str = "_ingested_at") -> str:
    """Idempotent upsert. A row only overwrites the target when it is strictly newer,
    so re-running a batch or a late file carrying older data never rolls a row back."""
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    return (
        f"MERGE INTO {target} AS t USING {source_view} AS s ON {on} "
        f"WHEN MATCHED AND s.`{sequence_col}` > t.`{sequence_col}` THEN UPDATE SET * "
        f"WHEN NOT MATCHED THEN INSERT *"
    )


def build_scd2_close_sql(target: str, source_view: str, key: str, ts_literal: str) -> str:
    """Step 1 of SCD2: end-date the current version when tracked attributes changed."""
    return (
        f"MERGE INTO {target} AS t USING {source_view} AS s "
        f"ON t.`{key}` = s.`{key}` AND t.is_current = true "
        f"WHEN MATCHED AND t.attr_hash <> s.attr_hash THEN "
        f"UPDATE SET t.is_current = false, t.valid_to = TIMESTAMP'{ts_literal}'"
    )


def build_scd2_insert_sql(target: str, source_view: str, key: str, columns, ts_literal: str) -> str:
    """Step 2 of SCD2: insert a current version for new keys and keys closed in step 1.
    Re-running with unchanged data inserts nothing (the anti join finds the current row)."""
    cols = ", ".join(f"s.`{c}`" for c in columns)
    return (
        f"INSERT INTO {target} "
        f"SELECT xxhash64(s.`{key}`, '{ts_literal}') AS customer_sk, {cols}, s.attr_hash, "
        f"TIMESTAMP'{ts_literal}' AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to, true AS is_current "
        f"FROM {source_view} AS s "
        f"LEFT ANTI JOIN (SELECT `{key}` FROM {target} WHERE is_current) AS c ON c.`{key}` = s.`{key}`"
    )


def merge_metrics(result_rows) -> dict:
    """Databricks MERGE returns one row of counts; tolerate runtimes that don't."""
    if not result_rows:
        return {"rows_inserted": None, "rows_updated": None}
    row = result_rows[0].asDict()
    return {
        "rows_inserted": row.get("num_inserted_rows"),
        "rows_updated": row.get("num_updated_rows"),
    }


def log_run(spark, table: str, **fields) -> None:
    row = {c: fields.get(c) for c in RUN_LOG_COLUMNS}
    spark.createDataFrame([row], schema=spark.table(table).schema).write.mode("append").saveAsTable(table)


def log_dq(spark, table: str, rows) -> None:
    rows = [{c: r.get(c) for c in DQ_COLUMNS} for r in rows]
    if rows:
        spark.createDataFrame(rows, schema=spark.table(table).schema).write.mode("append").saveAsTable(table)
