"""Step 5a: profile day-on-day jumps in Silver before writing a Gold jump rule.

A real move (e.g. a devaluation) jumps and STAYS: far from yesterday, close
to tomorrow. A typo jumps and COMES BACK: far from both, in the same
direction. This script measures both kinds. Reads only; writes nothing.

Usage (from the project root):
    python notebooks/profile_jumps.py
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

SILVER_PATH = ROOT / "data" / "silver" / "exchange_rates"

spark = (SparkSession.builder.master("local[*]").appName("profile-jumps")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Only rows Gold would use: valid rate order (this also drops the empty, unresolved rows)
valid = (spark.read.parquet(str(SILVER_PATH))
         .filter(F.col("rate_order_valid") == True)  # noqa: E712
         .select("currency", "ratedate", "centralrate"))

by_date = Window.partitionBy("currency").orderBy("ratedate")
moves = (valid
         .withColumn("prev_date", F.lag("ratedate").over(by_date))
         .withColumn("prev_rate", F.lag("centralrate").over(by_date))
         .withColumn("next_rate", F.lead("centralrate").over(by_date))
         .withColumn("log_from_prev", F.log(F.col("centralrate") / F.col("prev_rate")))
         .withColumn("log_to_next", F.log(F.col("centralrate") / F.col("next_rate")))
         # Reversal score: how far the value sits from BOTH neighbours in the same
         # direction. Zero when it moves and stays (or the neighbours disagree in sign).
         .withColumn("reversal", F.when(
             F.signum("log_from_prev") == F.signum("log_to_next"),
             F.least(F.abs("log_from_prev"), F.abs("log_to_next"))).otherwise(F.lit(0.0)))
         .withColumn("gap_days", F.datediff("ratedate", "prev_date"))
         .cache())

print("\n1. VALUES THAT JUMP AND COME BACK (reversal), by size")
for times in [1.1, 1.2, 1.5, 2, 3, 5, 10]:
    n = moves.filter(F.col("reversal") >= F.log(F.lit(float(times)))).count()
    print(f"  at least {times:>4}x away from both neighbours: {n}")

print("\n2. THE 40 LARGEST REVERSALS")
(moves.orderBy(F.desc("reversal"))
 .select("currency", "ratedate", "prev_rate", "centralrate", "next_rate",
         F.round(F.exp("reversal"), 2).alias("times_off"))
 .show(40, truncate=False))

print("3. THE 15 LARGEST US DOLLAR MOVES THAT STAYED (should be real events, not flagged)")
(moves.filter((F.col("currency") == "US DOLLAR") & (F.col("reversal") == 0))
 .orderBy(F.desc(F.abs("log_from_prev")))
 .select("ratedate", "prev_date", "prev_rate", "centralrate", "next_rate",
         F.round(F.exp("log_from_prev"), 3).alias("change_x"))
 .show(15, truncate=False))

print("4. GAPS BETWEEN CONSECUTIVE VALID ROWS (days), US DOLLAR")
(moves.filter(F.col("currency") == "US DOLLAR")
 .groupBy("gap_days").count().orderBy(F.desc("count"))
 .show(10, truncate=False))

spark.stop()
