"""Tests for Silver rules 1 and 2. Each test builds a tiny table by hand."""

import pytest

from silver import (
    BRONZE_SCHEMA,
    clean_currency_names,
    drop_excluded_currencies,
    build_silver,
    drop_identical_duplicates,
    flag_rate_order,
    resolve_conflicts,
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


# --- rules 3 and 4: conflicts -----------------------------------------------

CFA_NEIGHBOURS = [[60370, "CFA", "2025-11-26", "2.5377", "2.5477", "2.5577"],
                  [60400, "CFA", "2025-11-28", "2.5370", "2.5470", "2.5570"]]


def resolved_rows(spark, rows):
    out = resolve_conflicts(prepared(spark, rows))
    return {(r["currency"], str(r["ratedate"])): r for r in out.collect()}


def test_single_rows_pass_through_unchanged(spark):
    rows = resolved_rows(spark, CFA_NEIGHBOURS)
    row = rows[("CFA", "2025-11-26")]
    assert row["resolution"] == "single"
    assert row["centralrate"] == pytest.approx(2.5477)
    assert row["candidate_ids"] == [60370]


def test_large_conflict_keeps_the_plausible_copy_even_when_it_is_older(spark):
    # Real case: the later id carries 1958.95, not a CFA rate
    rows = resolved_rows(spark, CFA_NEIGHBOURS + [
        [60379, "CFA", "2025-11-27", "2.5257", "2.5357", "2.5457"],
        [60390, "CFA", "2025-11-27", "1958.2715", "1958.9501", "1959.6287"]])
    row = rows[("CFA", "2025-11-27")]
    assert row["resolution"] == "neighbour_check"
    assert row["id"] == 60379
    assert row["centralrate"] == pytest.approx(2.5357)
    assert row["candidate_ids"] == [60379, 60390]


def test_large_conflict_keeps_the_later_copy_when_that_is_the_plausible_one(spark):
    rows = resolved_rows(spark, [
        [49200, "SDR", "2022-03-14", "573.0", "574.4", "575.0"],
        [49300, "SDR", "2022-03-16", "573.5", "574.5", "575.5"],
        [49234, "SDR", "2022-03-15", "64.9", "65.2041", "65.5"],
        [49238, "SDR", "2022-03-15", "572.5", "573.1378", "573.8"]])
    assert rows[("SDR", "2022-03-15")]["id"] == 49238


def test_small_conflict_is_left_empty_and_flagged(spark):
    rows = resolved_rows(spark, [
        [55040, "CFA", "2024-02-21", "2.6200", "2.6300", "2.6400"],
        [55057, "CFA", "2024-02-22", "2.6244", "2.6344", "2.6444"],
        [55069, "CFA", "2024-02-22", "2.6261", "2.6361", "2.6461"]])
    row = rows[("CFA", "2024-02-22")]
    assert row["resolution"] == "conflict_unresolved"
    assert row["centralrate"] is None and row["id"] is None
    assert row["candidate_ids"] == [55057, 55069]


def test_large_conflict_without_neighbours_is_left_unresolved(spark):
    rows = resolved_rows(spark, [
        [1, "YEN", "2017-10-09", "2.70", "2.7072", "2.71"],
        [2, "YEN", "2017-10-09", "46.0", "46.0795", "46.1"]])
    assert rows[("YEN", "2017-10-09")]["resolution"] == "conflict_unresolved"


def test_large_conflict_where_even_the_best_copy_is_far_off_is_unresolved(spark):
    rows = resolved_rows(spark, [
        [10, "EURO", "2026-05-22", "1594", "1595", "1596"],
        [11, "EURO", "2026-05-25", "1300", "1300.0", "1301"],
        [12, "EURO", "2026-05-25", "1900", "1900.0", "1901"]])
    assert rows[("EURO", "2026-05-25")]["resolution"] == "conflict_unresolved"


def test_output_has_one_row_per_currency_and_date(spark):
    out = resolve_conflicts(prepared(spark, CFA_NEIGHBOURS + [
        [60379, "CFA", "2025-11-27", "2.5257", "2.5357", "2.5457"],
        [60390, "CFA", "2025-11-27", "1958.2715", "1958.9501", "1959.6287"]]))
    assert out.count() == out.select("currency", "ratedate").distinct().count() == 3


# --- rule 5: rate order ------------------------------------------------------

def test_flags_rows_in_order_as_valid(spark):
    df = flag_rate_order(prepared(spark, [[1, "US DOLLAR", "2026-10-08", "1331.1021", "1331.6021", "1332.1021"]]))
    assert df.first()["rate_order_valid"] is True


def test_flags_a_mistyped_central_rate_but_keeps_the_row(spark):
    # Real case: CFA, 23 Dec 2022, central 0.0714 between 0.7040 and 0.7240
    df = flag_rate_order(prepared(spark, [[51569, "CFA", "2022-12-23", "0.7040", "0.0714", "0.7240"]]))
    assert df.count() == 1
    assert df.first()["rate_order_valid"] is False


def test_unresolved_rows_get_an_empty_flag(spark):
    out = flag_rate_order(resolve_conflicts(prepared(spark, [
        [55040, "CFA", "2024-02-21", "2.6200", "2.6300", "2.6400"],
        [55057, "CFA", "2024-02-22", "2.6244", "2.6344", "2.6444"],
        [55069, "CFA", "2024-02-22", "2.6261", "2.6361", "2.6461"]])))
    row = [r for r in out.collect() if str(r["ratedate"]) == "2024-02-22"][0]
    assert row["rate_order_valid"] is None


# --- the whole Silver build --------------------------------------------------

def test_build_silver_applies_all_rules_and_reports_counts(spark):
    raw = make_raw(spark, [
        [1, "NAIRA", "2010-01-04", "1", "1", "1"],                       # excluded
        [2, "EURO\t", "2012-08-07", "1", "2", "3"],                      # name cleaned
        [3, "EURO", "2012-08-07", "1", "2", "3"],                         # identical copy
        [4, "CFA", "2022-12-23", "0.7040", "0.0714", "0.7240"],           # out of order
    ])
    silver, report = build_silver(raw)
    assert report == {"bronze_rows": 4, "excluded_currency_rows": 1, "identical_duplicates_dropped": 1}
    rows = {r["currency"]: r for r in silver.collect()}
    assert set(rows) == {"EURO", "CFA"}
    assert rows["EURO"]["id"] == 2 and rows["EURO"]["rate_order_valid"] is True
    assert rows["CFA"]["rate_order_valid"] is False
