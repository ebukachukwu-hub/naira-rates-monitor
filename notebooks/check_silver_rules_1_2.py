"""Run Silver rules 1 and 2 on the real Bronze snapshot and print the counts.

The counts should match what profiling found. Reads only; writes nothing.

Usage (from the project root):
    python notebooks/check_silver_rules_1_2.py
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
    read_bronze, type_columns,
)

BRONZE = ROOT / "data" / "bronze" / "exchange_rates"
latest = sorted(BRONZE.glob("*.json"))[-1]

spark = (SparkSession.builder.master("local[*]").appName("check-silver-1-2")
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

print("\nCurrencies after cleaning:")
deduped.groupBy("currency").agg(F.count("*").alias("rows"), F.min("ratedate").alias("first"),
                                F.max("ratedate").alias("last")).orderBy("rows").show(30, truncate=False)

spark.stop()
