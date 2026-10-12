from pyspark.sql import functions as F

from src import quality_rules as qr
from src.config import ENTITIES
from src.transforms import build_rules, parse_entity, select_clean, select_quarantine

ORDER_COLS = ENTITIES["orders"].columns


def _orders(spark, rows):
    cols = list(ORDER_COLS) + ["_rescued_data", "_ingested_at", "_source_file", "_load_id"]
    data = [r + (None, "2026-01-01 00:00:00", "f.csv", "load1") for r in rows]
    df = spark.createDataFrame(data, ", ".join(f"`{c}` string" for c in cols))
    return df.withColumn("_ingested_at", F.to_timestamp("_ingested_at"))


GOOD = ("o1", "c1", "delivered", "2017-10-02 10:56:33", "2017-10-02 11:07:15",
        "2017-10-04 19:55:00", "2017-10-10 21:25:13", "2017-10-18 00:00:00")


def _reasons(spark, rows):
    spec = ENTITIES["orders"]
    flagged = qr.assign_failure_reason(parse_entity(_orders(spark, rows), spec), build_rules(spec))
    return {r["_raw_order_id"]: r["failure_reason"] for r in flagged.collect()}


def test_good_order_passes(spark):
    assert _reasons(spark, [GOOD]) == {"o1": None}


def test_missing_key_is_quarantined(spark):
    blank_id = ("  ",) + GOOD[1:]
    no_customer = ("o2", None) + GOOD[2:]
    reasons = _reasons(spark, [blank_id, no_customer])
    assert reasons == {"  ": qr.MISSING_KEY, "o2": qr.MISSING_KEY}


def test_malformed_timestamp_is_not_silently_null(spark):
    bad = ("o3", "c1", "delivered", "2018-13-45 99:00:00") + GOOD[4:]
    assert _reasons(spark, [bad]) == {"o3": qr.INVALID_TIMESTAMP}


def test_empty_optional_timestamp_is_allowed(spark):
    not_delivered = ("o4", "c1", "shipped", GOOD[3], GOOD[4], GOOD[5], "", GOOD[7])
    assert _reasons(spark, [not_delivered]) == {"o4": None}


def test_invalid_status(spark):
    bad = ("o5", "c1", "lost_in_space") + GOOD[3:]
    renamed_column = ("o6", "c1", None) + GOOD[3:]
    assert _reasons(spark, [bad, renamed_column]) == {"o5": qr.INVALID_STATUS, "o6": qr.INVALID_STATUS}


def test_status_is_standardised(spark):
    spec = ENTITIES["orders"]
    shouty = ("o7", "c1", "  DELIVERED ") + GOOD[3:]
    row = parse_entity(_orders(spark, [shouty]), spec).first()
    assert row["order_status"] == "delivered"


def test_first_failing_rule_wins(spark):
    both = (None, "c1", "nonsense") + GOOD[3:]
    assert _reasons(spark, [both]) == {None: qr.MISSING_KEY}


def test_negative_value_and_number_parsing(spark):
    spec = ENTITIES["order_items"]
    cols = list(spec.columns) + ["_rescued_data", "_ingested_at", "_source_file", "_load_id"]
    rows = [
        ("o1", "1", "p1", "s1", "2017-10-06 11:07:15", "29.99", "8.72", None, None, "f", "l"),
        ("o2", "1", "p1", "s1", "2017-10-06 11:07:15", "-5.00", "8.72", None, None, "f", "l"),
        ("o3", "1", "p1", "s1", "2017-10-06 11:07:15", "abc", "8.72", None, None, "f", "l"),
        ("o4", "1.0", "p1", "s1", "2017-10-06 11:07:15", "1", "0", None, None, "f", "l"),
    ]
    df = spark.createDataFrame(rows, ", ".join(f"`{c}` string" for c in cols))
    parsed = parse_entity(df, spec).withColumn("_parent_order_exists", F.lit(True))
    valid, quarantine = qr.split_valid_quarantine(parsed, build_rules(spec))
    reasons = {r["_raw_order_id"]: r["failure_reason"] for r in quarantine.collect()}
    assert reasons == {"o2": qr.NEGATIVE_VALUE, "o3": qr.INVALID_NUMBER}
    kept = {r["order_id"]: r for r in select_clean(valid, spec).collect()}
    assert set(kept) == {"o1", "o4"}
    assert kept["o4"]["order_item_id"] == 1
    assert str(kept["o1"]["price"]) == "29.99"
    q = select_quarantine(quarantine, spec, "run1").collect()
    assert {r["price"] for r in q} == {"-5.00", "abc"}  # raw value kept for debugging


def test_orphan_rule(spark):
    from pyspark.sql import functions as F

    spec = ENTITIES["order_payments"]
    cols = list(spec.columns) + ["_rescued_data", "_ingested_at", "_source_file", "_load_id"]
    rows = [("o1", "1", "boleto", "1", "10.00", None, None, "f", "l"),
            ("ghost", "1", "boleto", "1", "10.00", None, None, "f", "l")]
    df = parse_entity(spark.createDataFrame(rows, ", ".join(f"`{c}` string" for c in cols)), spec)
    from src.transforms import add_parent_order_flag
    orders = spark.createDataFrame([("o1",)], "order_id string")
    flagged = qr.assign_failure_reason(add_parent_order_flag(df, orders), build_rules(spec))
    got = {r["order_id"]: r["failure_reason"] for r in flagged.select("order_id", "failure_reason").collect()}
    assert got == {"o1": None, "ghost": qr.ORPHAN_ORDER_PAYMENT}
    assert flagged.where(F.col("payment_type") != "boleto").count() == 0


def test_delivered_before_purchase_flag(spark):
    df = spark.createDataFrame(
        [("2017-01-02", "2017-01-01"), ("2017-01-01", "2017-01-05"), ("2017-01-01", None)],
        "purchased string, delivered string")
    flags = [r[0] for r in df.select(qr.delivered_before_purchase("delivered", "purchased")).collect()]
    assert flags == [True, False, False]
