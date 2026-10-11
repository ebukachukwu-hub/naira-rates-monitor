"""Silver: turn the raw CBN snapshot into one validated row per currency per date.

Every rule here comes from something seen when profiling the Bronze data
(notebooks/profile_snapshot.py).
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RATE_COLUMNS = ["buyingrate", "centralrate", "sellingrate"]
BRONZE_COLUMNS = ["id", "currency", "ratedate"] + RATE_COLUMNS

# Read Bronze with every column as text, exactly as the CBN sends it
BRONZE_SCHEMA = StructType([StructField(c, StringType()) for c in BRONZE_COLUMNS])

# Rule 1: different names stored for the same currency
ALIASES = {
    "JAPANESE YEN": "YEN",
    "POUND STERLING": "POUNDS STERLING",
    "DANISH KRONER": "DANISH KRONA",  # renamed by the CBN on 29 Aug 2012
}
# Rule 1: names that are not foreign currencies (NAIRA) or are unidentifiable (POESO)
EXCLUDED = ["NAIRA", "POESO"]


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    """Read one Bronze snapshot. The file is a single JSON array, hence multiLine."""
    return spark.read.schema(BRONZE_SCHEMA).option("multiLine", True).json(path)


def type_columns(raw: DataFrame) -> DataFrame:
    """Convert text ids, dates and rates to real types.

    Profiling found zero values that fail to convert, so any failure here
    means the source has changed: stop loudly rather than carry on.
    """
    typed = (raw
             .withColumn("id", F.col("id").try_cast("long"))
             .withColumn("ratedate", F.try_to_date(F.col("ratedate"), "yyyy-MM-dd")))
    for col in RATE_COLUMNS:
        typed = typed.withColumn(col, F.col(col).try_cast("double"))

    failed = {c: typed.filter(F.col(c).isNull() & raw[c].isNotNull()).count()
              for c in ["id", "ratedate"] + RATE_COLUMNS}
    failed = {c: n for c, n in failed.items() if n}
    if failed:
        raise ValueError(f"Values that failed to convert: {failed}")
    return typed


def clean_currency_names(df: DataFrame) -> DataFrame:
    """Rule 1: strip spaces and tabs, upper-case, apply aliases. Keep the raw name."""
    stripped = F.upper(F.regexp_replace(F.col("currency"), r"^\s+|\s+$", ""))
    alias_map = F.create_map(*[F.lit(x) for pair in ALIASES.items() for x in pair])
    return (df
            .withColumn("currency_raw", F.col("currency"))
            .withColumn("currency", F.coalesce(alias_map[stripped], stripped)))


def drop_excluded_currencies(df: DataFrame) -> tuple[DataFrame, int]:
    """Rule 1: remove NAIRA and POESO. Returns the kept rows and how many were dropped."""
    is_excluded = F.col("currency").isin(EXCLUDED)
    return df.filter(~is_excluded), df.filter(is_excluded).count()


def drop_identical_duplicates(df: DataFrame) -> tuple[DataFrame, int]:
    """Rule 2: where a currency and date repeat with the same rates, keep the lowest id."""
    same_values = Window.partitionBy("currency", "ratedate", *RATE_COLUMNS).orderBy("id")
    kept = (df.withColumn("_n", F.row_number().over(same_values))
              .filter(F.col("_n") == 1)
              .drop("_n"))
    return kept, df.count() - kept.count()


def resolve_conflicts(df: DataFrame, large_ratio: float = 1.05, window_days: int = 7,
                      max_offset: float = 0.10) -> DataFrame:
    """Rules 3 and 4: leave exactly one row per currency and date.

    - A currency-date with one row is kept as it is ("single").
    - Rule 3: if the copies differ by large_ratio or more, keep the copy whose
      central rate is closest to the median of the same currency's single rows
      within window_days either side ("neighbour_check").
    - Rule 4: if the copies differ by less than that, nothing shows which is
      official, so the rates are left empty ("conflict_unresolved").
    - Safeguard: if even the closest copy is more than max_offset away from its
      neighbours, or there are no neighbours, the conflict is also unresolved.

    Every output row lists the ids it was chosen from, for audit.
    """
    key = ["currency", "ratedate"]
    whole_group = Window.partitionBy(*key)
    counted = df.withColumn("_rows", F.count("*").over(whole_group))

    singles = (counted.filter(F.col("_rows") == 1).drop("_rows")
               .withColumn("resolution", F.lit("single"))
               .withColumn("candidate_ids", F.array(F.col("id")))
               .withColumn("conflict_ratio", F.lit(None).cast("double")))
    conflicts = counted.filter(F.col("_rows") > 1).drop("_rows")

    # Typical rate nearby: median of the same currency's single rows within the window
    reference = singles.select(F.col("currency").alias("_ref_currency"),
                               F.col("ratedate").alias("_ref_date"),
                               F.col("centralrate").alias("_ref_central"))
    nearby = (F.col("currency") == F.col("_ref_currency")) & \
             (F.abs(F.datediff(F.col("ratedate"), F.col("_ref_date"))) <= window_days)
    with_median = (conflicts.join(reference, nearby, "left")
                   .groupBy(*conflicts.columns)
                   .agg(F.median("_ref_central").alias("_neighbour_median")))

    ranked = (with_median
              .withColumn("_off_by", F.abs(F.col("centralrate") / F.col("_neighbour_median") - 1))
              .withColumn("conflict_ratio",
                          F.max("centralrate").over(whole_group) / F.min("centralrate").over(whole_group))
              .withColumn("candidate_ids", F.sort_array(F.collect_list("id").over(whole_group)))
              .withColumn("_rank", F.row_number().over(
                  whole_group.orderBy(F.col("_off_by").asc_nulls_last(), F.col("id")))))
    best = ranked.filter(F.col("_rank") == 1)

    resolvable = ((F.col("conflict_ratio") >= large_ratio)
                  & F.col("_off_by").isNotNull()
                  & (F.col("_off_by") <= max_offset))
    resolved = best.withColumn(
        "resolution", F.when(resolvable, "neighbour_check").otherwise("conflict_unresolved"))
    for col in ["id", "currency_raw"] + RATE_COLUMNS:
        resolved = resolved.withColumn(col, F.when(resolvable, F.col(col)))

    return singles.unionByName(resolved.select(*singles.columns))


def flag_rate_order(df: DataFrame) -> DataFrame:
    """Rule 5: flag rows where buying <= central <= selling fails. Never delete them.

    Profiling found rows where one rate looks mistyped while the others are
    fine, so the row is kept and the flag lets later steps decide. Empty
    (unresolved) rows get an empty flag: there is nothing to check.
    """
    in_order = (F.col("buyingrate") <= F.col("centralrate")) & (F.col("centralrate") <= F.col("sellingrate"))
    return df.withColumn("rate_order_valid",
                         F.when(F.col("centralrate").isNull(), F.lit(None)).otherwise(in_order))


SILVER_COLUMNS = ["currency", "ratedate", "buyingrate", "centralrate", "sellingrate",
                  "rate_order_valid", "resolution", "conflict_ratio", "candidate_ids",
                  "id", "currency_raw"]


def build_silver(raw: DataFrame) -> tuple[DataFrame, dict]:
    """Apply rules 1 to 5 in order. Returns the Silver table and a count for each step."""
    typed = type_columns(raw)
    named = clean_currency_names(typed)
    kept, excluded = drop_excluded_currencies(named)
    deduped, identical = drop_identical_duplicates(kept)
    silver = flag_rate_order(resolve_conflicts(deduped)).select(*SILVER_COLUMNS)
    report = {
        "bronze_rows": raw.count(),
        "excluded_currency_rows": excluded,
        "identical_duplicates_dropped": identical,
    }
    return silver, report
