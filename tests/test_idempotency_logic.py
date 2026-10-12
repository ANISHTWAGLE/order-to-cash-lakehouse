from datetime import datetime

from src.helpers import build_scd2_close_sql, build_scd2_insert_sql, build_upsert_sql
from src.quality_rules import dedupe_latest


def test_dedupe_keeps_latest_ingested_row(spark):
    df = spark.createDataFrame(
        [
            ("o1", "shipped", datetime(2026, 1, 1), "a.csv"),
            ("o1", "delivered", datetime(2026, 1, 2), "b.csv"),
            ("o2", "shipped", datetime(2026, 1, 1), "a.csv"),
        ],
        "order_id string, order_status string, _ingested_at timestamp, _source_file string",
    )
    kept, dropped = dedupe_latest(df, ["order_id"])
    assert {(r.order_id, r.order_status) for r in kept.collect()} == {("o1", "delivered"), ("o2", "shipped")}
    assert dropped.count() == 1


def test_dedupe_tie_breaks_on_file_name(spark):
    same_time = datetime(2026, 1, 1)
    df = spark.createDataFrame(
        [("o1", "x", same_time, "batch_01.csv"), ("o1", "y", same_time, "batch_02.csv")],
        "order_id string, v string, _ingested_at timestamp, _source_file string",
    )
    kept, _ = dedupe_latest(df, ["order_id"])
    assert [r.v for r in kept.collect()] == ["y"]


def test_upsert_only_overwrites_with_newer_rows():
    sql = build_upsert_sql("silver.orders", "src", ["order_id"])
    assert "ON t.`order_id` = s.`order_id`" in sql
    assert "WHEN MATCHED AND s.`_ingested_at` > t.`_ingested_at` THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql
    assert "DELETE" not in sql


def test_upsert_composite_key():
    sql = build_upsert_sql("silver.order_items", "src", ["order_id", "order_item_id"])
    assert "t.`order_id` = s.`order_id` AND t.`order_item_id` = s.`order_item_id`" in sql


def test_scd2_close_only_touches_changed_current_rows():
    sql = build_scd2_close_sql("gold.dim_customer", "src", "customer_unique_id", "2026-01-01 00:00:00")
    assert "t.is_current = true" in sql
    assert "t.attr_hash <> s.attr_hash" in sql
    assert "is_current = false" in sql


def test_scd2_insert_skips_keys_that_already_have_a_current_row():
    sql = build_scd2_insert_sql("gold.dim_customer", "src", "customer_unique_id",
                                ["customer_unique_id", "customer_city"], "2026-01-01 00:00:00")
    assert "LEFT ANTI JOIN (SELECT `customer_unique_id` FROM gold.dim_customer WHERE is_current)" in sql
    assert "true AS is_current" in sql
