"""Tests for Gold. Each test builds a small daily series by hand."""

from datetime import date, timedelta

import pytest
from pyspark.sql.types import (BooleanType, DateType, DoubleType, StringType,
                               StructField, StructType)

from gold import data_quality, flag_jumps, monthly_rates, valid_rates

SILVER_SCHEMA = StructType([
    StructField("currency", StringType()), StructField("ratedate", DateType()),
    StructField("centralrate", DoubleType()), StructField("rate_order_valid", BooleanType()),
    StructField("resolution", StringType()),
])


def series(spark, currency, start, values, valid=None, resolution=None):
    """One row per weekday from `start`, with the given central rates."""
    rows, day = [], date.fromisoformat(start)
    for i, value in enumerate(values):
        while day.weekday() >= 5:  # skip weekends, as the CBN does
            day += timedelta(days=1)
        ok = True if valid is None else valid[i]
        res = "single" if resolution is None else resolution[i]
        rows.append((currency, day, value, ok if value is not None else None, res))
        day += timedelta(days=1)
    return spark.createDataFrame(rows, SILVER_SCHEMA)


def flags(spark, values, **kwargs):
    out = flag_jumps(valid_rates(series(spark, "US DOLLAR", "2008-07-21", values)), **kwargs)
    return [r["jump_suspect"] for r in out.orderBy("ratedate").collect()]


# --- jump check ---------------------------------------------------------------

def test_isolated_typo_is_flagged(spark):
    # Real case: US dollar 1 Aug 2008, 66.09 between days of 116.09 (1.76x off)
    values = [116.09] * 5 + [66.09] + [116.09] * 5
    assert flags(spark, values) == [False] * 5 + [True] + [False] * 5


def test_clustered_errors_do_not_flag_the_correct_value_between_them(spark):
    # Real case: Danish krona 18-20 June 2025
    values = [239.0, 239.2, 239.4, 1782.2547, 238.42, 1781.3681, 238.8, 238.9, 239.1]
    assert flags(spark, values) == [False, False, False, True, False, True, False, False, False]


def test_a_real_move_that_stays_is_not_flagged(spark):
    # Shape of June 2016: about 1.42x in one day, then stays
    values = [196.5] * 6 + [279.5, 283.5, 283.0, 282.5, 282.0, 281.5]
    assert not any(flags(spark, values))


def test_too_few_neighbours_is_not_flagged(spark):
    assert flags(spark, [116.09, 66.09, 116.09]) == [False, False, False]


def test_flagged_rows_are_not_usable(spark):
    values = [116.09] * 5 + [66.09] + [116.09] * 5
    out = flag_jumps(valid_rates(series(spark, "US DOLLAR", "2008-07-21", values)))
    row = out.filter("centralrate = 66.09").first()
    assert row["usable"] is False
    assert row["reference_rate"] == pytest.approx(116.09)
    assert row["jump_ratio"] == pytest.approx(116.09 / 66.09)


def test_invalid_and_empty_rows_never_reach_gold(spark):
    silver = series(spark, "CFA", "2022-12-19", [0.72, 0.0714, None],
                    valid=[True, False, None],
                    resolution=["single", "single", "conflict_unresolved"])
    assert valid_rates(silver).count() == 1


# --- monthly ------------------------------------------------------------------

def test_monthly_uses_usable_rows_only(spark):
    values = [116.0] * 5 + [66.0] + [118.0] * 5   # 21 Jul to 4 Aug 2008
    daily = flag_jumps(valid_rates(series(spark, "US DOLLAR", "2008-07-21", values)))
    july = monthly_rates(daily).filter("month = date'2008-07-01'").first()
    assert july["usable_days"] == 8          # 9 weekdays in July, the 66.0 typo excluded
    assert july["month_end_rate"] == pytest.approx(118.0)
    assert july["month_end_date"] == date(2008, 7, 31)


# --- data quality ---------------------------------------------------------------

def test_data_quality_counts_every_kind_of_row(spark):
    values = [116.09] * 5 + [66.09] + [116.09] * 5 + [120.0, None]
    valid = [True] * 11 + [False, None]
    resolution = ["single"] * 12 + ["conflict_unresolved"]
    silver = series(spark, "US DOLLAR", "2008-07-21", values, valid, resolution)
    daily = flag_jumps(valid_rates(silver))
    q = data_quality(silver, daily).filter("year = 2008").first()
    assert q["silver_rows"] == 13
    assert q["rate_order_breaks"] == 1
    assert q["unresolved_conflicts"] == 1
    assert q["jump_suspects"] == 1
    assert q["usable_rows"] == 10
