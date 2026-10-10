"""Bronze: download the CBN exchange-rate history and save it untouched."""

import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

URL = "https://www.cbn.gov.ng/api/GetAllExchangeRates"
ROOT = Path(__file__).resolve().parents[1]
BRONZE = ROOT / "data" / "bronze" / "exchange_rates"

# 1. Download
request = urllib.request.Request(URL, headers={"User-Agent": "naira-rates-monitor"})
with urllib.request.urlopen(request, timeout=120) as response:
    body = response.read()

# 2. Check it is valid JSON before saving anything
rows = json.loads(body)

# 3. Save the bytes exactly as received, named by the UTC fetch time
fetched_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
BRONZE.mkdir(parents=True, exist_ok=True)
path = BRONZE / f"{fetched_at}.json"
path.write_bytes(body)

print(f"Saved {len(rows)} rows to {path}")