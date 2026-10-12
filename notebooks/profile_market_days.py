"""Step 5c, part 1: are large deviations market-wide (real) or isolated (errors)?

When the naira itself moves, every currency's rate against it moves the same
way on the same day. A typo hits one currency. For every value at least 1.2x
from its reference, this counts how many OTHER currencies moved at least 1.15x
in the same direction that day. Reads Gold daily_rates; writes nothing.

Usage (from the project root):
    python notebooks/profile_market_days.py
"""

import math
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

DAILY_PATH = ROOT / "data" / "gold" / "daily_rates"
CANDIDATE = 1.2   # values this far off are what the new rule would examine
COMPANION = 1.15  # another currency this far off, same direction, counts as moving too

spark = (SparkSession.builder.master("local[*]").appName("profile-market-days")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Signed deviation: positive = above its reference, negative = below
daily = (spark.read.parquet(str(DAILY_PATH))
         .filter(F.col("reference_rate").isNotNull())
         .withColumn("deviation", F.log(F.col("centralrate") / F.col("reference_rate"))))

per_date = daily.groupBy("ratedate").agg(
    F.count("*").alias("published"),
    F.sum(F.when(F.col("deviation") >= math.log(COMPANION), 1).otherwise(0)).alias("up"),
    F.sum(F.when(F.col("deviation") <= -math.log(COMPANION), 1).otherwise(0)).alias("down"))

candidates = (daily.filter(F.col("jump_ratio") >= CANDIDATE)
              .join(per_date, "ratedate")
              # other currencies moving the same way (minus 1: the candidate itself)
              .withColumn("others_same_way",
                          F.when(F.col("deviation") > 0, F.col("up")).otherwise(F.col("down")) - 1)
              .cache())

print(f"\n1. VALUES AT LEAST {CANDIDATE}x OFF: how many other currencies moved with them?")
band = (F.when(F.col("others_same_way") == 0, "0 others (alone)")
         .when(F.col("others_same_way") <= 2, "1-2 others")
         .when(F.col("others_same_way") <= 5, "3-5 others")
         .otherwise("6 or more others"))
(candidates.withColumn("band", band).groupBy("band")
 .agg(F.count("*").alias("values"), F.sum(F.col("jump_suspect").cast("int")).alias("already_flagged_at_1_5x"))
 .orderBy("band").show(truncate=False))

print("2. DATES WHERE 2 OR MORE CURRENCIES WERE AT LEAST 1.2x OFF (candidate real events)")
(candidates.groupBy("ratedate", "published", "up", "down")
 .agg(F.count("*").alias("candidates"),
      F.round(F.avg("jump_ratio"), 2).alias("avg_times_off"),
      F.concat_ws(", ", F.sort_array(F.collect_list("currency"))).alias("currencies"))
 .filter(F.col("candidates") >= 2)
 .orderBy("ratedate").show(60, truncate=80))

print("3. VALUES AT LEAST 1.2x OFF THAT MOVED ALONE (should look like errors)")
(candidates.filter(F.col("others_same_way") == 0)
 .select("currency", "ratedate", "centralrate", F.round("reference_rate", 4).alias("reference"),
         F.round("jump_ratio", 2).alias("times_off"), "jump_suspect", "published")
 .orderBy("jump_ratio").show(100, truncate=False))

spark.stop()
