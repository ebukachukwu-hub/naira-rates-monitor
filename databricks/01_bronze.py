# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: fetch the CBN exchange-rate history
# MAGIC Saves the API response untouched to the `raw` volume, named by UTC fetch time.
# MAGIC Snapshots are never overwritten, so later runs can detect revisions.

# COMMAND ----------

import json
import urllib.request
from datetime import datetime, timezone

URL = "https://www.cbn.gov.ng/api/GetAllExchangeRates"
BRONZE_DIR = "/Volumes/workspace/naira_rates/raw/exchange_rates"

# COMMAND ----------

request = urllib.request.Request(URL, headers={"User-Agent": "naira-rates-monitor"})
with urllib.request.urlopen(request, timeout=120) as response:
    body = response.read()

rows = json.loads(body)  # stop here if the response is not valid JSON

stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
dbutils.fs.mkdirs(BRONZE_DIR)
path = f"{BRONZE_DIR}/{stamp}.json"
with open(path, "wb") as f:
    f.write(body)

print(f"Saved {len(rows)} rows to {path}")
