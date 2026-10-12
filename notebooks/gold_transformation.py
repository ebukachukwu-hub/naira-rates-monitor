"""Silver -> Gold: daily rates with the jump check, monthly rates, data quality.

Usage (from the project root):
    python notebooks/gold_transformation.py
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
from gold import data_quality, flag_jumps, monthly_rates, valid_rates  # noqa: E402
from local_io import clear_local_output  # noqa: E402

SILVER_PATH = ROOT / "data" / "silver" / "exchange_rates"
GOLD_PATH = ROOT / "data" / "gold"

spark = (SparkSession.builder.master("local[*]").appName("naira-rates-gold")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

silver = spark.read.parquet(str(SILVER_PATH)).cache()
daily = flag_jumps(valid_rates(silver)).cache()
monthly = monthly_rates(daily)
quality = data_quality(silver, daily)

# ---------------------------------------------------------------- review
print("\n1. JUMP CHECK")
print("Valid Silver rows checked:", daily.count())
print("Suspected errors (flagged, not usable):", daily.filter("jump_suspect").count())

print("\nNewly caught by the market-adjusted rule (own deviation 1.2x to 1.5x): check by eye")
(daily.filter("jump_suspect AND own_ratio < 1.5")
 .select("currency", "ratedate", "centralrate", F.round("reference_rate", 4).alias("reference"),
         F.round("jump_ratio", 2).alias("raw_x"), F.round("market_ratio", 2).alias("market_x"),
         F.round("own_ratio", 2).alias("own_x"))
 .orderBy("ratedate").show(60, truncate=False))

print("Market-wide days (median move across currencies at least 1.15x): should be real events")
(daily.filter("market_ratio >= 1.15")
 .groupBy("ratedate").agg(F.round(F.first("market_ratio"), 2).alias("market_x"),
                          F.count("*").alias("currencies"),
                          F.sum(F.col("jump_suspect").cast("int")).alias("flagged"))
 .orderBy("ratedate").show(30, truncate=False))

print("Yen, 18-27 Feb 2015 (an error on top of a real move?):")
(daily.filter("currency = 'YEN' AND ratedate BETWEEN '2015-02-18' AND '2015-02-27'")
 .select("ratedate", "centralrate", F.round("reference_rate", 4).alias("reference"),
         F.round("market_ratio", 2).alias("market_x"), F.round("own_ratio", 2).alias("own_x"),
         "jump_suspect")
 .orderBy("ratedate").show(truncate=False))

print("Danish krona, 16-24 June 2025 (only the two wrong values should be flagged):")
(daily.filter("currency = 'DANISH KRONA' AND ratedate BETWEEN '2025-06-16' AND '2025-06-24'")
 .select("ratedate", "centralrate", F.round("reference_rate", 2).alias("reference"), "jump_suspect")
 .orderBy("ratedate").show(truncate=False))

print("2. DATA QUALITY, ALL YEARS, BY CURRENCY")
(quality.groupBy("currency")
 .agg(*[F.sum(c).alias(c) for c in ["silver_rows", "resolved_conflicts", "unresolved_conflicts",
                                    "rate_order_breaks", "jump_suspects", "usable_rows"]])
 .withColumn("usable_share", F.round(F.col("usable_rows") / F.col("silver_rows"), 4))
 .orderBy("usable_share").show(20, truncate=False))

# ---------------------------------------------------------------- save
for name, table in [("daily_rates", daily), ("monthly_rates", monthly), ("data_quality", quality)]:
    path = GOLD_PATH / name
    clear_local_output(path)
    table.write.mode("overwrite").parquet(str(path))
    print(f"{name}: {spark.read.parquet(str(path)).count()} rows written to {path}")

spark.stop()
