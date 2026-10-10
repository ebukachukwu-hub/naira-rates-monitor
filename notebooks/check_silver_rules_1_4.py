"""Run Silver rules 1 to 4 on the real Bronze snapshot and print the counts.

The counts should match what profiling found. Reads only; writes nothing.

Usage (from the project root):
    python notebooks/check_silver_rules_1_4.py
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from silver import (  # noqa: E402
    clean_currency_names, drop_excluded_currencies, drop_identical_duplicates,
    read_bronze, resolve_conflicts, type_columns,
)

BRONZE = ROOT / "data" / "bronze" / "exchange_rates"
latest = sorted(BRONZE.glob("*.json"))[-1]

spark = (SparkSession.builder.master("local[*]").appName("check-silver-1-4")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

raw = read_bronze(spark, str(latest))
print("Snapshot:", latest.name)
print("Bronze rows:", raw.count())

named = clean_currency_names(type_columns(raw))
kept, excluded = drop_excluded_currencies(named)
print("Rows excluded (NAIRA, POESO):", excluded)

deduped, identical = drop_identical_duplicates(kept)
print("Identical duplicates dropped:", identical)
print("Rows remaining:", deduped.count())

conflicts = deduped.groupBy("currency", "ratedate").count().filter(F.col("count") > 1)
print("Currency-dates still repeated (conflicts for rules 3 and 4):", conflicts.count())

silver = resolve_conflicts(deduped).cache()
print("\nSilver rows:", silver.count())
print("Distinct currency-dates:", silver.select("currency", "ratedate").distinct().count())

print("\nRows by resolution:")
silver.groupBy("resolution").count().orderBy("resolution").show(truncate=False)

print("Conflicts resolved by the neighbour check:")
(silver.filter(F.col("resolution") == "neighbour_check")
 .select("currency", "ratedate", "id", "centralrate", "candidate_ids",
         F.round("conflict_ratio", 2).alias("conflict_ratio"))
 .orderBy("ratedate").show(30, truncate=False))

print("Unresolved conflicts that were LARGE (stopped by the safeguard):")
(silver.filter((F.col("resolution") == "conflict_unresolved") & (F.col("conflict_ratio") >= 1.05))
 .select("currency", "ratedate", "candidate_ids", F.round("conflict_ratio", 2).alias("conflict_ratio"))
 .show(30, truncate=False))

spark.stop()
