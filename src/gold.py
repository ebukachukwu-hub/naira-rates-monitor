"""Gold: analysis-ready exchange-rate tables built from Silver.

Pure functions: DataFrame in, DataFrame out. Reading and writing happen in
the scripts, so the same code runs locally and on Databricks.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

JUMP_WINDOW = 5         # valid values either side used as the reference
JUMP_FACTOR = 1.5       # flag a value this many times above or below the reference
MIN_NEIGHBOURS = 4      # fewer reference values than this: too little evidence to flag


def valid_rates(silver: DataFrame) -> DataFrame:
    """Rows Gold may use: rate order valid (this also drops the empty, unresolved rows)."""
    return (silver.filter(F.col("rate_order_valid") == True)  # noqa: E712
            .select("currency", "ratedate", "centralrate"))


def _median_of(array_col: str) -> F.Column:
    """Median of a numeric array column (null if the array is empty)."""
    s = F.array_sort(F.col(array_col))
    n = F.size(s)
    low = F.element_at(s, (n / 2).cast("int"))
    high = F.element_at(s, (n / 2).cast("int") + 1)
    return (F.when(n == 0, F.lit(None))
             .when(n % 2 == 1, F.element_at(s, ((n + 1) / 2).cast("int")))
             .otherwise((low + high) / 2))


def flag_jumps(rates: DataFrame, window: int = JUMP_WINDOW, factor: float = JUMP_FACTOR,
               min_neighbours: int = MIN_NEIGHBOURS) -> DataFrame:
    """Flag values far from the median of the valid values around them.

    The reference is the median of up to `window` valid values before and
    `window` after (same currency, excluding the value itself), counted in
    rows, so weekends and holidays do not shrink it. A value is a suspected
    error if it is at least `factor` times above or below that median.

    A real move that stays cannot be flagged unless it roughly triples the
    rate in one day: the reference then straddles the old and new levels.
    """
    order = Window.partitionBy("currency").orderBy("ratedate")
    before = order.rowsBetween(-window, -1)
    after = order.rowsBetween(1, window)
    with_ref = (rates
                .withColumn("_around", F.concat(F.collect_list("centralrate").over(before),
                                                F.collect_list("centralrate").over(after)))
                .withColumn("neighbours", F.size("_around"))
                .withColumn("reference_rate", _median_of("_around")))
    ratio = F.greatest(F.col("centralrate") / F.col("reference_rate"),
                       F.col("reference_rate") / F.col("centralrate"))
    return (with_ref
            .withColumn("jump_ratio", ratio)
            .withColumn("jump_suspect",
                        (F.col("neighbours") >= min_neighbours) & (F.col("jump_ratio") >= factor))
            .withColumn("usable", ~F.col("jump_suspect"))
            .drop("_around"))


def monthly_rates(daily: DataFrame) -> DataFrame:
    """Per currency per month, from usable rows only: mean, month-end rate, and coverage."""
    usable = daily.filter(F.col("usable")).withColumn("month", F.trunc("ratedate", "month"))
    return (usable.groupBy("currency", "month")
            .agg(F.avg("centralrate").alias("mean_rate"),
                 F.max_by("centralrate", "ratedate").alias("month_end_rate"),
                 F.max("ratedate").alias("month_end_date"),
                 F.count("*").alias("usable_days")))


def data_quality(silver: DataFrame, daily: DataFrame) -> DataFrame:
    """Per currency per year: how many Silver rows were usable, flagged or empty, and why."""
    flags = daily.select("currency", "ratedate", "jump_suspect")
    joined = silver.join(flags, ["currency", "ratedate"], "left")
    year = F.year("ratedate").alias("year")
    return (joined.groupBy("currency", year)
            .agg(F.count("*").alias("silver_rows"),
                 F.sum(F.when(F.col("resolution") == "conflict_unresolved", 1).otherwise(0))
                  .alias("unresolved_conflicts"),
                 F.sum(F.when(F.col("resolution") == "neighbour_check", 1).otherwise(0))
                  .alias("resolved_conflicts"),
                 F.sum(F.when(F.col("rate_order_valid") == False, 1).otherwise(0))  # noqa: E712
                  .alias("rate_order_breaks"),
                 F.sum(F.when(F.col("jump_suspect") == True, 1).otherwise(0))  # noqa: E712
                  .alias("jump_suspects"),
                 F.sum(F.when(F.col("jump_suspect") == False, 1).otherwise(0))  # noqa: E712
                  .alias("usable_rows"))
            .withColumn("usable_share", F.round(F.col("usable_rows") / F.col("silver_rows"), 4)))
