"""Step 3: profile the latest Bronze snapshot. Reads only; writes nothing."""

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BRONZE = ROOT / "data" / "bronze" / "exchange_rates"

# Load the newest snapshot, keeping every value as text
latest = sorted(BRONZE.glob("*.json"))[-1]
raw = pd.read_json(latest, dtype=False)
print("Snapshot:", latest.name)

# 1. Shape
print("\n1. SHAPE")
print("Columns:", list(raw.columns))
print("Rows:", len(raw))

# 2. Types: convert a copy, count what fails
df = raw.copy()
df["ratedate"] = pd.to_datetime(df["ratedate"], errors="coerce")
for col in ["buyingrate", "centralrate", "sellingrate"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")
print("\n2. TYPES")
print("Dates that failed to convert:", df["ratedate"].isna().sum())
print("Rates that failed to convert:",
      df[["buyingrate", "centralrate", "sellingrate"]].isna().sum().to_dict())
print("Date range:", df["ratedate"].min().date(), "to", df["ratedate"].max().date())

# 3. Currencies
print("\n3. CURRENCIES (rows, first date, last date)")
summary = df.groupby("currency")["ratedate"].agg(["count", "min", "max"])
print(summary.sort_values("count").to_string())

# 4. Duplicates
print("\n4. DUPLICATES")
print("Repeated ids:", df["id"].duplicated().sum())
print("Repeated (currency, date) pairs:", df.duplicated(["currency", "ratedate"]).sum())

# 5. Logic
print("\n5. LOGIC")
wrong_order = df[(df["buyingrate"] > df["centralrate"]) | (df["centralrate"] > df["sellingrate"])]
print("Rows where buying <= central <= selling fails:", len(wrong_order))
print(wrong_order.head(10).to_string(index=False))

usd = df[df["currency"] == "US DOLLAR"]
spread = (usd["sellingrate"] - usd["buyingrate"]).round(4)
print("\nUS dollar spread (selling minus buying), most common values:")
print(spread.value_counts().head(10).to_string())


# 6. Currency names exactly as stored
print("\n6. CURRENCY NAMES AS STORED")
for name, count in raw["currency"].value_counts().sort_index().items():
    print(f"{name!r:30} {count}")

# 7. Duplicates after removing hidden spaces and tabs
df["currency_clean"] = df["currency"].str.strip().str.upper()
key = ["currency_clean", "ratedate"]
dups = df[df.duplicated(key, keep=False)].sort_values(key + ["id"])

print("\n7. DUPLICATES (after stripping spaces and tabs)")
print("Repeated (currency, date) pairs:", df.duplicated(key).sum())
print("Rows involved:", len(dups))

values = ["buyingrate", "centralrate", "sellingrate"]
distinct = dups.groupby(key)[values].nunique().max(axis=1)
print("Pairs where the repeats carry the same rates:", (distinct == 1).sum())
print("Pairs where the repeats disagree:", (distinct > 1).sum())

print("\nDuplicates by year:")
print(dups.groupby(dups["ratedate"].dt.year).size().to_string())

print("\nFirst 20 duplicate rows:")
print(dups[["id", "currency", "ratedate"] + values].head(20).to_string(index=False))

# 8. Conflicting duplicates: is the later id the plausible one?
import numpy as np

conflicts = dups.groupby(key).filter(lambda g: g[values].nunique().max() > 1).copy()
central = conflicts.groupby(key)["centralrate"]
conflicts["kind"] = np.where(central.transform("max") / central.transform("min") < 1.05,
                             "small (<5%)", "large (5%+)")
conflicts["is_latest_id"] = conflicts["id"] == conflicts.groupby(key)["id"].transform("max")

# Reference: same currency, within 7 days, excluding any duplicated rows
reference = df[~df.duplicated(key, keep=False)]

def neighbour_median(row):
    nearby = reference[(reference["currency_clean"] == row["currency_clean"])
                       & ((reference["ratedate"] - row["ratedate"]).abs() <= pd.Timedelta(days=7))]
    return nearby["centralrate"].median()

conflicts["neighbour_median"] = conflicts.apply(neighbour_median, axis=1)
conflicts["off_by"] = (conflicts["centralrate"] / conflicts["neighbour_median"] - 1).abs()
conflicts["is_closest"] = conflicts["off_by"] == conflicts.groupby(key)["off_by"].transform("min")

print("\n8. CONFLICTING DUPLICATES")
groups = conflicts[conflicts["is_latest_id"]].groupby("kind")
print(pd.DataFrame({
    "pairs": groups.size(),
    "later id is the closest copy": groups["is_closest"].sum(),
}).to_string())

print("\nGap between ids within each pair (most common):")
gaps = conflicts.groupby(key)["id"].agg(lambda s: s.max() - s.min())
print(gaps.value_counts().head(8).to_string())

print("\nAll large conflicts:")
large = conflicts[conflicts["kind"] == "large (5%+)"]
print(large[["id", "currency_clean", "ratedate", "centralrate", "neighbour_median",
             "is_latest_id", "is_closest"]].to_string(index=False))