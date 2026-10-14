# Naira Rates Monitor

![tests](https://github.com/ebukachukwu-hub/naira-rates-monitor/actions/workflows/tests.yml/badge.svg)

A PySpark pipeline that collects the Central Bank of Nigeria's official
exchange rates every day, cleans them through Bronze, Silver and Gold layers,
and measures how reliable the published data is. It runs locally and as a
scheduled job on Databricks, from the same tested code.

The headline finding: the CBN's official rate history contains errors of up to
10,000 times the true value, swapped currencies and mislabelled duplicates.
The pipeline catches 210 problem values out of 62,205 and keeps every decision
traceable to the original records.

## The questions

1. **How reliable is the CBN's published exchange-rate data?** Answered by
   this phase.
2. **What real return have Nigerian Treasury bills paid once inflation is
   taken out?** Phase 2: needs inflation and T-bill data, not yet added.

## Data Source

Central Bank of Nigeria exchange rates, from the JSON endpoint behind the
CBN's exchange-rate page: `https://www.cbn.gov.ng/api/GetAllExchangeRates`.
No key is needed, and one call returns the full history.

- One row per currency per working day: buying, central and selling rates
- 13 currencies published each working day
- 10 December 2001 to the present; the snapshot used for the figures below
  was fetched on 10 October 2026 and holds 62,333 rows up to 9 October 2026
- Every value, including rates, arrives as text

## Architecture

```
                  LOCAL                          DATABRICKS (Free Edition)

BRONZE            data/bronze/*.json             Unity Catalog volume
                  untouched API response,        workspace.naira_rates.raw
                  one file per fetch
                        |                                |
SILVER            data/silver (Parquet)          silver_exchange_rates (Delta)
                  one validated row per
                  currency per date
                        |                                |
GOLD              data/gold (Parquet)            gold_daily_rates
                                                 gold_monthly_rates
                                                 gold_data_quality (Delta)
```

All rules live in `src/silver.py` and `src/gold.py`. The Databricks notebooks
import them from the repository's Git folder, so the code the tests check is
the code that runs in production. The Databricks job `naira-rates-daily`
runs Bronze, Silver and Gold in order on a schedule.

Bronze snapshots are never overwritten. Each fetch is saved as a new file
named by its UTC fetch time, so two snapshots can be compared to detect the
CBN revising past rates.

The 31 unit tests run on GitHub Actions on every push and pull request
(Python 3.14, Java 17, PySpark 4.2.0).

## How the rules were chosen

Every rule came from profiling the data first. Two obvious rules turned out
to be wrong once tested against the data, and the evidence is kept in the
`notebooks/` scripts.

### Silver

**1. Currency names.** The same currency is stored under several names:
trailing spaces and tabs (`'EURO\t'`, `'US DOLLAR '`), variant spellings
(`JAPANESE YEN` for `YEN`, `POUND STERLING` for `POUNDS STERLING`) and a
rename (`DANISH KRONER` until 28 August 2012, `DANISH KRONA` after). Names are
stripped and mapped through an explicit alias table, and the raw name is kept
for audit. `NAIRA` (8 rows) and `POESO` (3 rows) are excluded.

**2. Identical duplicates.** 37 rows repeat a currency, date and all three
rates exactly. One copy is kept.

**3 and 4. Conflicting duplicates.** 80 currency-dates appear twice with
different rates. The obvious rule, "keep the later entry, it is probably the
correction", was tested first and failed: it picked the plausible copy in
only 4 of 21 large conflicts and 25 of 59 small ones. Instead:

- **Large conflicts (copies 5% or more apart, 21 cases):** keep the copy
  closest to the median of the same currency's rates within a week either
  side. In every case one copy fits and the other is wildly off, such as a
  CFA rate of 1,958.95 on 27 November 2025, when the CFA was worth about 2.54.
- **Small conflicts (under 5% apart, 59 cases):** nothing shows which copy
  the CBN meant, so the rate is left empty and flagged. Nothing is imputed.
- **Safeguard:** if even the closest copy is more than 10% from its
  neighbours, the conflict is also left unresolved. No current case trips it.

Every row lists the source ids it was chosen from.

**5. Rate order.** 76 rows break buying ≤ central ≤ selling. They are kept
and flagged, not deleted, because often only one rate is mistyped. Gold does
not use them: in three US dollar rows the broken value is the central rate
itself, including ₦1,376.80 on 9 December 2003, when the dollar was about
₦136.80.

Result: 62,205 rows, exactly one per currency per date (62,125 single,
21 resolved conflicts, 59 unresolved).

### Gold

**Jump check.** Some errors pass every Silver rule because all three rates
were mistyped the same way: the Danish krona at 198,024 on 14 November 2008,
against about 19.6 either side. Gold compares each value with the median of
the 5 valid values before and after it, then removes the day's market move
before deciding.

- **Market move.** When the naira itself moves, every currency's rate moves
  with it on the same day. A typo hits one currency. The market move is the
  median deviation across all currencies that day (zero if fewer than six
  were published). A value is flagged if it is at least 1.2 times off after
  removing it.
- **Why not a simpler rule.** Comparing only with the day before and after
  flagged the correct Danish krona rate of 238.42 on 19 June 2025, because it
  sat between two wrong values. A flat 1.5x threshold missed about ten
  isolated errors; a flat 1.2x threshold would have flagged real events such
  as 29 January 2024, when all 12 currencies moved together.
- **Linked errors.** CFA and WAUA often go wrong together on the same day
  while other currencies stay still. The CFA franc is pegged to the euro
  (655.957 per euro), so it cannot move 25% against the naira unless the euro
  does. These pairs are flagged.

The check finds 75 errors. It recognises eight market-wide days as real
events, including the naira float (June 2016), the exchange-rate unification
(₦462.88 on 9 June 2023 to ₦632.27 on 14 June) and the January 2024 reforms.

Gold produces three tables:

| Table | Rows | Contents |
|---|---|---|
| `gold_daily_rates` | 62,070 | Valid rows with the jump check and a `usable` flag |
| `gold_monthly_rates` | 3,074 | Monthly mean and month-end rate per currency, usable rows only |
| `gold_data_quality` | 266 | Per currency per year: usable, flagged and empty rows, and why |

## Findings

### 1. 99.66% of the official history is usable

Of 62,205 currency-days, 210 are unusable: 59 unresolved conflicts, 76
rate-order breaks and 75 jump errors.

| Currency | Usable share | Jump errors | Rate-order breaks |
|---|---|---|---|
| CFA | 99.32% | 26 | 12 |
| South African rand | 99.45% | 4 | 7 |
| WAUA | 99.57% | 13 | 5 |
| US dollar | 99.80% | 2 | 6 |
| UAE dirham | 100% | 0 | 0 |

The CFA franc is the least reliable series; the US dollar, the most watched,
is among the cleanest.

### 2. The errors are large and of a few recognisable kinds

- **Extra or missing digits:** the pound sterling at 2,248,433 on 30 October
  2003, against about 224.
- **Swapped currencies:** on 27 November 2003 the WAUA and CFA rates were
  entered in each other's rows.
- **Mislabelled duplicates:** a second row for one currency carrying another
  currency's rate.
- **Stale values:** the WAUA rate did not move with the market when the naira
  floated in June 2016.

### 3. Errors cluster in time

27 of the 76 rate-order breaks are from 2002 and 2003. Duplicate entries
peak in 2024 (76 rows), the year of the largest exchange-rate moves.

### 4. The spread is a fixed markup

For the US dollar, selling minus buying is exactly ₦1.00 in 6,006 of 6,081
rows (98.8%). It is a pricing convention, not a market signal, so it is not
analysed. Departures from it are a useful error check.

## Limitations

- **A single currency moving 40% or more on its own in one day** would be
  flagged on the day before the move: that value sits (1 + s) / 2 from a
  reference that straddles both levels. Moves of the whole naira are not
  affected, because the market adjustment removes them.
- **The 5%, 10% and 1.2x thresholds are judgement calls.** The South African
  rand conflict on 22 February 2024 sits right at the 5% line. A systematic
  sensitivity test of the thresholds has not been run yet.
- **Missing days are not yet checked** against a Nigerian public holiday
  calendar.
- **Revisions:** snapshots are kept, but the comparison between them is not
  yet built.

## How to Run

Requirements: Python 3.14 and a Java runtime supported by PySpark 4.2.

From the project root:

```
python -m pip install -r requirements.txt
python -m pytest -v
python src/fetch.py
python notebooks/silver_transformation.py
python notebooks/gold_transformation.py
```

Profiling scripts that read the saved data and write nothing:

```
python notebooks/profile_snapshot.py
python notebooks/check_silver_rules_1_4.py
python notebooks/profile_jumps.py
python notebooks/profile_market_days.py
```

On Databricks Free Edition:

1. Create the schema and volume:
   `CREATE SCHEMA IF NOT EXISTS workspace.naira_rates;`
   `CREATE VOLUME IF NOT EXISTS workspace.naira_rates.raw;`
2. Clone this repository into the workspace as a Git folder.
3. Run `databricks/01_bronze`, `02_silver` and `03_gold` in order on
   serverless compute, or create a job with the three as dependent tasks.

## Project Structure

```
naira-rates-monitor/
├── .github/workflows/tests.yml     runs the tests on push and pull request
├── data/                           (not committed)
│   ├── bronze/exchange_rates/      untouched snapshots, one per fetch
│   ├── silver/exchange_rates/      written by silver_transformation.py
│   └── gold/                       written by gold_transformation.py
├── databricks/                     Databricks notebooks (Delta tables)
│   ├── 01_bronze.py
│   ├── 02_silver.py
│   └── 03_gold.py
├── notebooks/                      local pipeline and profiling
│   ├── profile_snapshot.py         Bronze profile behind the Silver rules
│   ├── check_silver_rules_1_4.py
│   ├── silver_transformation.py
│   ├── profile_jumps.py            jump profile behind the Gold rule
│   ├── profile_market_days.py      market-wide versus isolated moves
│   └── gold_transformation.py
├── src/
│   ├── fetch.py                    Bronze fetch
│   ├── silver.py                   Silver rules 1 to 5
│   ├── gold.py                     jump check, monthly rates, data quality
│   └── local_io.py                 local output handling (Windows)
├── tests/
│   ├── conftest.py
│   ├── test_silver.py              19 tests
│   └── test_gold.py                12 tests
├── pytest.ini
└── requirements.txt
```

## Status

- [x] Bronze: untouched daily snapshots of the CBN API
- [x] Silver: five profiled rules, one row per currency per date
- [x] Gold: market-adjusted jump check, monthly rates, data-quality table
- [x] 31 unit tests, run by GitHub Actions on every push
- [x] Databricks Free Edition: Bronze, Silver and Gold as Delta tables,
      run by a scheduled job
- [ ] Compare snapshots to detect revisions to past rates
- [ ] Check missing days against a public holiday calendar
- [ ] Sensitivity test of the 5%, 10% and 1.2x thresholds
- [ ] Phase 2: inflation and T-bill rates, and real T-bill returns
