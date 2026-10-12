"""Small, named data-quality rules.

Each rule returns a boolean Column that is TRUE when a row FAILS the rule.
`assign_failure_reason` applies rules in order; the first failure wins, so
every quarantined row carries exactly one reason.
"""
from functools import reduce
from operator import or_

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

MISSING_KEY = "missing_key"
INVALID_TIMESTAMP = "invalid_timestamp"
INVALID_NUMBER = "invalid_number"
NEGATIVE_VALUE = "negative_value"
INVALID_STATUS = "invalid_status"
ORPHAN_ORDER_ITEM = "orphan_order_item"
ORPHAN_ORDER_PAYMENT = "orphan_order_payment"


def _any(conditions):
    conditions = list(conditions)
    return reduce(or_, conditions) if conditions else F.lit(False)


def is_blank(column: Column) -> Column:
    return column.isNull() | (F.trim(column.cast("string")) == "")


def missing_key(columns) -> Column:
    return _any(is_blank(F.col(c)) for c in columns)


def unparseable(pairs) -> Column:
    """pairs: (raw_column, parsed_column). Raw had a value but parsing gave NULL."""
    return _any(~is_blank(F.col(raw)) & F.col(parsed).isNull() for raw, parsed in pairs)


def negative_value(columns) -> Column:
    return _any(F.coalesce(F.col(c) < 0, F.lit(False)) for c in columns)


def invalid_status(column: str, allowed) -> Column:
    return F.col(column).isNull() | ~F.col(column).isin(*allowed)


def orphan(parent_exists_column: str) -> Column:
    return ~F.coalesce(F.col(parent_exists_column), F.lit(False))


def assign_failure_reason(df: DataFrame, rules) -> DataFrame:
    """rules: list of (reason, failing_condition). Adds `failure_reason` (NULL = passed)."""
    reason = F.lit(None).cast("string")
    for name, condition in reversed(list(rules)):
        reason = F.when(condition, F.lit(name)).otherwise(reason)
    return df.withColumn("failure_reason", reason)


def split_valid_quarantine(df: DataFrame, rules):
    flagged = assign_failure_reason(df, rules)
    valid = flagged.where(F.col("failure_reason").isNull()).drop("failure_reason")
    quarantine = flagged.where(F.col("failure_reason").isNotNull())
    return valid, quarantine


def dedupe_latest(df: DataFrame, keys, order_by=("_ingested_at", "_source_file")):
    """Keep the latest row per business key. Returns (kept, dropped).

    Two copies in the same file have no ordering signal, so a hash of the whole
    row breaks the tie: the choice is arbitrary but identical on every re-run.
    """
    tie_break = F.xxhash64(*[F.col(f"`{c}`") for c in df.columns])
    window = Window.partitionBy(*keys).orderBy(
        *[F.col(c).desc_nulls_last() for c in order_by], tie_break.desc())
    ranked = df.withColumn("_rn", F.row_number().over(window))
    kept = ranked.where("_rn = 1").drop("_rn")
    dropped = ranked.where("_rn > 1").drop("_rn")
    return kept, dropped


def delivered_before_purchase(delivered: str, purchased: str) -> Column:
    """Logged as a flag in dq_results, never used to drop a row."""
    return F.coalesce(F.col(delivered) < F.col(purchased), F.lit(False))
