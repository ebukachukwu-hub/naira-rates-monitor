# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: latest Bronze snapshot to a validated Delta table
# MAGIC Uses `build_silver` from `src/silver.py`, the same code the tests check.

# COMMAND ----------

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(os.getcwd()).parent / "src"))

from pyspark.sql import functions as F
from silver import build_silver, read_bronze

BRONZE_DIR = "/Volumes/workspace/naira_rates/raw/exchange_rates"
SILVER_TABLE = "workspace.naira_rates.silver_exchange_rates"

# COMMAND ----------

snapshots = sorted(f.path for f in dbutils.fs.ls(BRONZE_DIR) if f.name.endswith(".json"))
latest = snapshots[-1]
print("Snapshot:", latest)

silver, report = build_silver(read_bronze(spark, latest))

n_rows = silver.count()
n_keys = silver.select("currency", "ratedate").distinct().count()
assert n_rows == n_keys, f"{n_rows - n_keys} repeated (currency, date) keys in Silver"

for name, value in report.items():
    print(f"{name}: {value}")
print(f"silver_rows: {n_rows}")

# COMMAND ----------

(silver.withColumn("_source_file", F.lit(latest))
       .withColumn("_built_at", F.current_timestamp())
       .write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(SILVER_TABLE))

display(spark.table(SILVER_TABLE).groupBy("resolution").count())
