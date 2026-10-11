"""Bronze (latest CBN snapshot) -> Silver (validated, one row per currency per date).

Usage (from the project root):
    python notebooks/silver_transformation.py
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
from local_io import clear_local_output  # noqa: E402
from silver import build_silver, read_bronze  # noqa: E402

BRONZE = ROOT / "data" / "bronze" / "exchange_rates"
SILVER_PATH = ROOT / "data" / "silver" / "exchange_rates"

spark = (SparkSession.builder.master("local[*]").appName("naira-rates-silver")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 1. Read the newest Bronze snapshot
snapshot = sorted(BRONZE.glob("*.json"))[-1]
raw = read_bronze(spark, str(snapshot))

# 2. Build Silver (rules 1 to 5)
silver, report = build_silver(raw)
silver = silver.cache()

# 3. Validate before saving: stop if the grain is broken
n_rows = silver.count()
n_keys = silver.select("currency", "ratedate").distinct().count()
assert n_rows == n_keys, f"{n_rows - n_keys} repeated (currency, date) keys in Silver"

# 4. Report
print(f"\nSnapshot: {snapshot.name}")
for name, value in report.items():
    print(f"{name}: {value}")
print(f"silver_rows: {n_rows}")

print("\nRows by resolution:")
silver.groupBy("resolution").count().orderBy("resolution").show(truncate=False)

print("Rate order (buying <= central <= selling):")
silver.groupBy("rate_order_valid").count().orderBy("rate_order_valid").show(truncate=False)

print("Rows that break the rate order (kept, flagged):")
(silver.filter(F.col("rate_order_valid") == False)  # noqa: E712
 .select("currency", "ratedate", "buyingrate", "centralrate", "sellingrate")
 .orderBy("ratedate").show(100, truncate=False))

# 5. Save
clear_local_output(SILVER_PATH)
silver.write.mode("overwrite").partitionBy("currency").parquet(str(SILVER_PATH))
print(f"Silver written to {SILVER_PATH}")

# 6. Read it back to prove the saved table matches
saved = spark.read.parquet(str(SILVER_PATH))
assert saved.count() == n_rows, "Saved Silver does not match the built table"
print(f"Read back {saved.count()} rows: OK")

spark.stop()
