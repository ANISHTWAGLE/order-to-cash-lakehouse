import json
from datetime import date
from pathlib import Path

import pytest

from src.config import FX_END, FX_FALLBACK_FILE, FX_START
from src.fx import forward_fill, forward_fill_df, frankfurter_url, parse_frankfurter_payload

FRI, SAT, SUN, MON = date(2017, 1, 6), date(2017, 1, 7), date(2017, 1, 8), date(2017, 1, 9)


def test_url_is_one_range_request():
    url = frankfurter_url("2016-08-25", "2018-10-31")
    assert url == "https://api.frankfurter.dev/v1/2016-08-25..2018-10-31?from=BRL&to=USD"


def test_parse_payload():
    payload = {"base": "BRL", "rates": {"2017-01-06": {"USD": 0.31}, "2017-01-09": {"USD": 0.32}}}
    assert parse_frankfurter_payload(payload) == {FRI: 0.31, MON: 0.32}


def test_weekend_is_filled_from_friday():
    out = forward_fill({FRI: 0.31, MON: 0.32}, FRI, MON)
    assert out == [(FRI, 0.31, False), (SAT, 0.31, True), (SUN, 0.31, True), (MON, 0.32, False)]


def test_start_on_weekend_uses_earlier_seed():
    out = forward_fill({FRI: 0.31, MON: 0.32}, SAT, SUN)
    assert [r[1] for r in out] == [0.31, 0.31]


def test_no_seed_rate_raises_instead_of_using_future_rate():
    with pytest.raises(ValueError):
        forward_fill({MON: 0.32}, SAT, MON)


def test_spark_forward_fill_matches_reference(spark):
    rates = spark.createDataFrame([(FRI, 0.31), (MON, 0.32)], "rate_date date, rate double")
    rows = forward_fill_df(spark, rates, "2017-01-06", "2017-01-09").orderBy("rate_date").collect()
    assert [(r.rate_date, r.rate, r.is_filled) for r in rows] == forward_fill({FRI: 0.31, MON: 0.32}, FRI, MON)


def test_committed_fallback_file_covers_every_order_day():
    payload = json.loads((Path(__file__).parents[1] / FX_FALLBACK_FILE).read_text())
    rates = parse_frankfurter_payload(payload)
    # Olist purchase dates run 2016-09-04 .. 2018-10-17.
    filled = forward_fill(rates, date(2016, 9, 4), date(2018, 10, 17))
    assert all(rate > 0 for _, rate, _ in filled)
    assert min(rates) <= date.fromisoformat(FX_START) or min(rates) <= date(2016, 9, 4)
    assert max(rates) <= date.fromisoformat(FX_END)
