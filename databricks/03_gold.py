# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: daily rates with the jump check, monthly rates, data quality
# MAGIC Uses `src/gold.py`, the same code the tests check.

# COMMAND ----------

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(os.getcwd()).parent / "src"))

from pyspark.sql import functions as F
from gold import data_quality, flag_jumps, monthly_rates, valid_rates

SCHEMA = "workspace.naira_rates"

# COMMAND ----------

silver = spark.table(f"{SCHEMA}.silver_exchange_rates").drop("_source_file", "_built_at")
daily = flag_jumps(valid_rates(silver))

tables = {
    "gold_daily_rates": daily,
    "gold_monthly_rates": monthly_rates(daily),
    "gold_data_quality": data_quality(silver, daily),
}
for name, df in tables.items():
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.{name}")
    print(f"{name}: {spark.table(f'{SCHEMA}.{name}').count()} rows")

# COMMAND ----------

# Usable share by currency, least reliable first
display(spark.table(f"{SCHEMA}.gold_data_quality")
        .groupBy("currency")
        .agg(F.sum("silver_rows").alias("silver_rows"), F.sum("usable_rows").alias("usable_rows"))
        .withColumn("usable_share", F.round(F.col("usable_rows") / F.col("silver_rows"), 4))
        .orderBy("usable_share"))
