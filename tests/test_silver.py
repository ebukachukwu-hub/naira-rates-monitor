"""Tests for Silver rules 1 and 2. Each test builds a tiny table by hand."""

import pytest

from silver import (
    BRONZE_SCHEMA,
    clean_currency_names,
    drop_excluded_currencies,
    drop_identical_duplicates,
    type_columns,
)


def make_raw(spark, rows):
    """Build a Bronze-like table: every value is text, as the CBN sends it."""
    return spark.createDataFrame([[str(v) for v in row] for row in rows], BRONZE_SCHEMA)


def prepared(spark, rows):
    return clean_currency_names(type_columns(make_raw(spark, rows)))


# --- typing -----------------------------------------------------------------

def test_type_columns_converts_text_to_numbers_and_dates(spark):
    raw = make_raw(spark, [[1, "EURO", "2026-10-08", "1489.1039", "1489.6633", "1490.2226"]])
    row = type_columns(raw).first()
    assert row["centralrate"] == pytest.approx(1489.6633)
    assert str(row["ratedate"]) == "2026-10-08"
    assert row["id"] == 1


def test_type_columns_fails_loudly_on_a_bad_rate(spark):
    raw = make_raw(spark, [[1, "EURO", "2026-10-08", "1489.1", "N/A", "1490.2"]])
    with pytest.raises(ValueError, match="centralrate"):
        type_columns(raw)


# --- rule 1: currency names -------------------------------------------------

def test_strips_hidden_tabs_and_spaces(spark):
    df = prepared(spark, [[1, "EURO\t", "2012-08-07", "1", "2", "3"],
                          [2, "US DOLLAR ", "2012-08-09", "1", "2", "3"]])
    assert [r["currency"] for r in df.orderBy("id").collect()] == ["EURO", "US DOLLAR"]


def test_applies_aliases_including_after_stripping(spark):
    df = prepared(spark, [[1, "JAPANESE YEN", "2012-08-16", "1", "2", "3"],
                          [2, "POUND STERLING ", "2012-08-22", "1", "2", "3"],
                          [3, "DANISH KRONER", "2012-08-28", "1", "2", "3"]])
    names = [r["currency"] for r in df.orderBy("id").collect()]
    assert names == ["YEN", "POUNDS STERLING", "DANISH KRONA"]


def test_keeps_the_raw_name_for_audit(spark):
    df = prepared(spark, [[1, "EURO\t", "2012-08-07", "1", "2", "3"]])
    assert df.first()["currency_raw"] == "EURO\t"


def test_excludes_naira_and_poeso_and_counts_them(spark):
    df = prepared(spark, [[1, "NAIRA", "2010-01-04", "1", "1", "1"],
                          [2, "POESO", "2009-07-01", "1", "2", "3"],
                          [3, "EURO", "2009-07-01", "1", "2", "3"]])
    kept, dropped = drop_excluded_currencies(df)
    assert dropped == 2
    assert [r["currency"] for r in kept.collect()] == ["EURO"]


# --- rule 2: identical duplicates -------------------------------------------

def test_drops_identical_duplicates_keeping_the_lowest_id(spark):
    # 9999 must win: as text, "9999" would sort after "24926"
    df = prepared(spark, [[24927, "CFA", "2013-10-11", "0.3003", "0.3103", "0.3203"],
                          [24926, "CFA ", "2013-10-11", "0.3003", "0.3103", "0.3203"],
                          [9999, "CFA", "2013-10-11", "0.3003", "0.3103", "0.3203"]])
    kept, dropped = drop_identical_duplicates(df)
    assert dropped == 2
    assert [r["id"] for r in kept.collect()] == [9999]


def test_leaves_conflicting_duplicates_for_later_rules(spark):
    df = prepared(spark, [[55057, "CFA", "2024-02-22", "2.6244", "2.6344", "2.6444"],
                          [55069, "CFA", "2024-02-22", "2.6261", "2.6361", "2.6461"]])
    kept, dropped = drop_identical_duplicates(df)
    assert dropped == 0
    assert kept.count() == 2
