# NY IEDR – Customer Usage Gridflow

This repo is my take on the IEDR data pipeline case study: take messy, utility-specific files and turn them into a clean, consistent model that the IEDR app (and future analytics/ML) can use without caring which utility sent the data.

---

## Problem Framing

The case provides sample data from two different NY utilities. They describe the same concepts (service points, meters, interval usage) but in very different ways:

- **Utility 1**
  - Uses `SERVICE_POINT_ID`
  - Sends interval timestamps as ISO-like strings
  - Includes the service point directly on the intervals

- **Utility 2**
  - Uses `premise_id` instead of service point ID
  - Sends timestamps as integers like `20250123`
  - Only gives `meter_id` on intervals; you have to join through meters to get to the service point

The IEDR application **doesn’t** want to know about any of that. It just wants:

- A **common data model** across utilities  
- A **single usage table** to query for customer usage patterns  
- A **summary layer** for “peak/pit usage by location and day”  
- Clear signals when data is **missing, mis-joined, or out of range**

This project is meant to demonstrate that end-to-end: from raw CSVs → standardized model → usage fact table → daily experience layer → data quality snapshot.

---

## Architecture Overview

I modeled this as a medallion-style pipeline, similar to how I’d implement it in Databricks:

1. **Landing (Raw)**
2. **Standardized (Common Model)**
3. **Product (Detailed Usage Fact)**
4. **Experience (Daily Summaries / UI-Ready)**

---

### 1. Landing (Raw)

- Location: `data/raw/utility1`, `data/raw/utility2`
- Behavior: load CSVs as-is (no transformations), one directory per utility:

  - `utility*_service_points.csv`
  - `utility*_meters.csv`
  - `utility*_intervals.csv`

- Code: [`gridflow/io_landing.py`](gridflow/io_landing.py)

This layer is about making I/O explicit and testable, not clever.

**Example raw volumes:**

- **UTILITY1**
  - ~1,039 service points  
  - ~2,842 meters  
  - 211 interval rows  

- **UTILITY2**
  - ~967 service points  
  - ~2,665 meters  
  - 43,152 interval rows (noisy, highly duplicated)

---

### 2. Standardized (Common Model)

This layer hides the utility-specific quirks and presents a **common schema** for:

- Service points  
- Meters  
- Interval readings  

Key pieces live in [`gridflow/io_standardized.py`](gridflow/io_standardized.py).

#### Service points – `build_standardized_service_points`

- **Utility 1**
  - `SERVICE_POINT_ID` → `service_point_id`
  - Address columns (`SERVICE_POINT_STREET`, `SERVICE_POINT_CITY`, etc.) → unified location fields

- **Utility 2**
  - `premise_id` → `service_point_id`
  - `premise_house_num`, `premise_street`, `premise_city`, `premise_zip`, `premise_region` → aligned to the same structure

**Result:** a single `service_points` table with:

- `utility_id`
- `service_point_id`
- Address/location fields
- Installed/removed/created timestamps where available

> In the standardized layer, some Utility 2 service points are dropped because they never link to any meter/interval data. The DQ snapshot calls this out explicitly.

#### Meters – `build_standardized_meters`

- **Utility 1**
  - `meter_id` normalized to string
  - No direct service point link in the file, so `service_point_id` is left null  
    (future enhancement could infer this from additional data)

- **Utility 2**
  - `meter_id` and `meter_number` normalized
  - `premise_id` carried through as `service_point_id`
  - Installed/removed/created timestamps retained

**Result:** a single `meters` table with:

- `utility_id`
- `meter_id` (string)
- `service_point_id` (populated for Utility 2)
- Meter type, category, timestamps

#### Intervals – `build_standardized_intervals`

- **Utility 1**
  - Carries `service_delivery_point_id` directly → `service_point_id`
  - Timestamps taken from an ISO-like `timestamp` column

- **Utility 2**
  - Timestamps are integers like `20250123`
  - Explicitly parsed with `pd.to_datetime(..., format="%Y%m%d")`
  - Only has `meter_id` → we join through the standardized `meters` table to get `service_point_id`

Both utilities are then enriched with a common set of fields:

- `utility_id`
- `service_point_id`
- `meter_id`
- `interval_start_ts`
- `interval_end_ts` (computed from duration)
- `duration_seconds`
- `value` (usage amount)
- `quality`
- `channel` (e.g. kWh, kVARh, etc.)

**Deduplication**

Utility 2’s interval file is extremely noisy and includes:

- multiple channels per meter/timestamp, and  
- many true duplicate rows.

The final dedup key is:

```python
["utility_id", "service_point_id", "meter_id", "interval_start_ts", "channel"]
````

This preserves **distinct measurement channels** (e.g. kWh vs kVARh) while collapsing true duplicates.

**Resulting standardized intervals:**

* Total standardized interval rows: **6,389**

---

### 3. Product Layer – Detailed Usage

Code:

* [`gridflow/io_product.py`](gridflow/io_product.py)
* [`gridflow/pipelines/build_product.py`](gridflow/pipelines/build_product.py)

Here I join:

* Standardized service points
* Standardized meters
* Standardized intervals

into a single **usage fact** table.

**Keys (conceptually):**

* `utility_id`
* `service_point_id`
* `meter_id`
* `interval_start_ts` / `interval_end_ts`
* `channel`

**Metrics:**

* `value` (usage)
* `duration_seconds`

**Context:**

* `quality`
* Location fields (`city`, `zip`, region, etc.)
* Meter and service point metadata

This table is the “one place to go” for any usage analytics or UI queries at interval granularity.

Resulting Product layer:

* Rows: **6,389** (1:1 with standardized intervals)

---

### 4. Experience Layer – Daily Usage Summaries

Code:

* [`gridflow/io_experience.py`](gridflow/io_experience.py)
* [`gridflow/pipelines/build_experience.py`](gridflow/pipelines/build_experience.py)

The Experience layer rolls the detailed usage into **daily buckets** per service point.

For each `(utility_id, service_point_id, day)`:

* `bucket_start`, `bucket_end`
* `total_usage`
* `interval_count`
* `peak_usage_value`, `peak_usage_ts`
* `pit_usage_value`, `pit_usage_ts`

The UI only needs a **single API call** against this table to answer questions like:

* “What’s the daily usage profile for this location?”
* “When was peak usage yesterday?”
* “Which locations have very low usage (pits) that might indicate issues or DER opportunities?”

Resulting Experience layer:

* **1,287** daily summaries
* For **532** distinct service points

---

## Data Quality & Monitoring

Data quality is a first-class concern in this design.

I added a dedicated DQ pipeline:
[`gridflow/pipelines/print_dq_snapshot.py`](gridflow/pipelines/print_dq_snapshot.py)

It prints a snapshot like:

```text
IEDR Data Quality Snapshot
Run: 2025-11-28 23:09:22
--------------------------------------------------

ROW COUNTS
  Layer                  UTILITY1   UTILITY2      Total
  Service Points            1,039        195      1,234
  Meters                      291        750      1,041
  Intervals                   211      6,178      6,389
  Product                     211      6,178      6,389
  Experience                  211      1,076      1,287

SOURCE DATA QUALITY
  UTILITY2 interval duplicates: 36,974 (86% of raw)
  These are same meter/timestamp/channel with different values.

REFERENTIAL INTEGRITY
  Intervals -> Meters:         0 orphan meter_ids ✓
  Intervals -> Service Points: 126 orphan SP IDs (1,016 rows) ⚠
    UTILITY2: 126 missing premises

COMPLETENESS (% non-null)
  Intervals: all critical fields complete
  Meters: service_point_id:72%
  Service Points: all critical fields complete

INTERVAL VALUES
  Range: 0 to 999,956
  Mean: 36,362, Median: 8,230
  Zeros: 174 (2.7%)
  Negatives: 0
  Outliers (IQR): 199 (3.1%)

TIMESTAMP RANGE
  2025-01-22 to 2025-02-21 (30 days)
  Nulls: 0, Pre-2020: 0

EXPERIENCE LAYER
  1,287 daily summaries for 532 service points
  Avg daily usage: 180,509
  Peak value seen: 999,956

--------------------------------------------------
SUMMARY
  Records ready for analysis: 84% (5,373 / 6,389)
  Issues to address:
    - 126 SP IDs missing from UTILITY2 (source issue)
    - UTILITY2 has 86% duplicate readings (clarify with utility)
--------------------------------------------------
```

What this captures:

* **Volume by utility and by layer**
  Quickly shows what each utility is sending and where it ends up (raw → standardized → product → experience).

* **Null checks on critical fields**
  IDs and timestamps are validated before downstream consumers rely on them.

* **Timestamp sanity**
  Range checks catch parsing issues or obviously wrong dates (e.g., pre-2020 leakage).

* **Join coverage (referential integrity)**
  Verifies that every interval can:

  * Join to a meter (`meter_id`)
  * Join to a service point (`service_point_id` vs `service_points`)

  The “126 orphan SP IDs / 1,016 rows” is exactly the kind of thing that should trigger attention and, in production, either:

  * quarantine those rows, or
  * send a DQ alert back to the utility.

* **Value checks & outliers**

  * Negative usage (shouldn’t happen)
  * Zeros and outliers via an IQR rule
  * Simple distribution stats (min, max, mean, median) to spot weirdness fast

---

## PII & Security Considerations (Design Intent)

The case hints at PII (addresses, etc.). In a real implementation:

* Address and customer-identifying fields in `service_points` would be tagged as **PII**.
* External-facing views would expose only:

  * masked fields (e.g., partial address), or
  * aggregated geography (ZIP3, feeder, census block, etc.).
* Access to raw tables would be restricted to pipeline/service accounts and governed users.
* In Databricks/Snowflake, I’d layer on:

  * column-level security / dynamic masking, and
  * role-based access controls around any PII-bearing columns.

This repo keeps PII inline for simplicity, but the model is structured to support those controls.

---

## Bugs I Found & How I Fixed Them

I deliberately ran sanity checks *after* wiring everything up and found a couple of real bugs plus some upstream data issues.

### 1. Missing Channel in Interval Dedup Key

**File:** `gridflow/io_standardized.py` (interval standardization)

**Problem**

Initially, I deduplicated intervals on:

```python
["utility_id", "service_point_id", "meter_id", "interval_start_ts"]
```

That looked reasonable, but Utility 2 sends **multiple channels per meter/timestamp** (e.g., kWh, kVARh). With `channel` missing from the key, different measurement types were collapsed into a single row.

**Impact**

* Before: ~2,530 interval rows (lost ~94% of detail)
* After fix: **6,389** interval rows (correct once true source duplicates are removed)

**Fix**

Add `channel` to the dedup subset:

```python
combined = combined.drop_duplicates(
    subset=[
        "utility_id",
        "service_point_id",
        "meter_id",
        "interval_start_ts",
        "channel",
    ]
)
```

This preserves one row per `(utility, SP, meter, time, channel)` while still collapsing true duplicates.

---

### 2. Utility 2 Intervals Not Joining (Early Iteration)

**Symptom (early iteration)**

* Utility 2 intervals were coming out with `service_point_id = NaN` after the join.

**Root Cause**

Two issues combined:

1. Building the Utility 2 intervals DataFrame in a way that created a column with all nulls and then tried to join on it.
2. Inconsistent typing on join keys (`meter_id` as string vs numeric across different frames).

**Fix**

* Build the Utility 2 base DataFrame in one go, rather than assigning a scalar then a Series.
* Normalize `meter_id` to **string** on both sides before the join:

```python
u2_base = pd.DataFrame(
    {
        "utility_id": "UTILITY2",
        "meter_id": u2_raw["meter_id"].astype(str),
        # other columns...
    }
)

meters["meter_id"] = meters["meter_id"].astype(str)

intervals = u2_base.merge(
    meters[["utility_id", "meter_id", "service_point_id"]],
    on=["utility_id", "meter_id"],
    how="left",
)
```

The current snapshot shows:

* Intervals → Meters: **0 orphan meter_ids**
* Intervals → Service Points: 84% linkage, with the remaining failures explained by missing premises in the source data (see below).

---

### 3. Timestamp Parsing for Utility 2

**Symptom (early iteration)**

* Some timestamps landed in 1970 or had obviously wrong values.

**Root Cause**

* Utility 2 timestamps are integers like `20250123`. Without explicit parsing, pandas can interpret them incorrectly or silently coerce to weird dates.

**Fix**

Parse them explicitly with the correct format and coercion:

```python
u2_ts = pd.to_datetime(
    u2_raw["timestamp"].astype(str),
    format="%Y%m%d",
    errors="coerce",
)
```

The current DQ snapshot confirms:

* Timestamp range: **2025-01-22 → 2025-02-21**
* Nulls: `0`
* Pre-2020: `0`

---

## Source Data Quality Findings (Not Pipeline Bugs)

These are issues with the **source data**, not the pipeline logic, but the pipeline surfaces them clearly.

### 1. Missing Premises from Utility 2

* Utility 2’s meters reference `premise_id`s that don’t exist in their own `service_points` file.
* This shows up as:

  * **126** orphan `service_point_id`s in intervals
  * **1,016** affected interval rows (about 16% of Utility 2’s standardized interval data)

The pipeline:

* Links all intervals to meters (100% coverage),
* But only links 84% of intervals all the way to service points.

In a real IEDR environment, this would:

* Feed a DQ dashboard
* Drive tickets back to the utility for missing premise data
* Potentially quarantine those intervals from “official” analytical products

### 2. Massive Interval Duplication in Utility 2

* About **86%** of Utility 2’s raw interval rows are duplicates:

  * same meter
  * same timestamp
  * same channel
  * sometimes **different values**

The pipeline currently:

* Keeps the **first occurrence**
* Surfaces the duplicate count and percentage in the DQ snapshot

In production, we’d work with the utility to confirm:

* whether last-write-wins should apply,
* whether one feed is “corrected” vs “original”,
* and codify that rule explicitly in the standardization logic.

---

## Final Metrics

After fixes and DQ analysis:

| Metric                              | Value                    |
| ----------------------------------- | ------------------------ |
| Total standardized intervals        | 6,389                    |
| Records “ready for analysis”        | 5,373 (84% of intervals) |
| Intervals → Meters linkage          | 100%                     |
| Intervals → Service Points linkage  | 84%                      |
| Orphan service_point_ids            | 126 (1,016 rows)         |
| Utility 2 interval duplicates (raw) | 36,974 (86% of raw rows) |
| High-severity source DQ issues      | 1 (missing premises)     |
| Known pipeline bugs after fixes     | 0 (tests passing)        |

---

## Project Structure

```text
data/
  raw/
    utility1/
      utility1_service_points.csv
      utility1_meters.csv
      utility1_intervals.csv
    utility2/
      utility2_service_points.csv
      utility2_meters.csv
      utility2_intervals.csv

gridflow/
  io_landing.py        # load raw files into dataframes
  io_standardized.py   # map U1/U2 into a common schema (SPs, meters, intervals)
  io_product.py        # build detailed usage fact table (Product layer)
  io_experience.py     # build daily summaries with peak/pit usage (Experience layer)
  pipelines/
    load_landing.py        # pipeline entrypoint: load raw data
    build_standardized.py  # pipeline entrypoint: build standardized layer
    build_product.py       # pipeline entrypoint: build usage fact table
    build_experience.py    # pipeline entrypoint: build experience layer
    print_dq_snapshot.py   # pipeline entrypoint: run DQ snapshot

tests/
  test_service_points_unique.py   # check SP IDs are unique per utility
  test_intervals_linkage.py       # check intervals link correctly to meters/SPs
```

---

## How to Run It

Requirements: Python 3.10+ and `pandas` / `pytest`.

From the repo root:

```bash
# create and activate venv (example)
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1

pip install pandas pytest
```

Then:

```bash
# 1) Load raw CSVs into the landing layer (dataframes / artifacts)
python -m gridflow.pipelines.load_landing

# 2) Build standardized service_points, meters, intervals
python -m gridflow.pipelines.build_standardized

# 3) Build detailed usage (Product layer)
python -m gridflow.pipelines.build_product

# 4) Build daily summaries (Experience layer)
python -m gridflow.pipelines.build_experience

# 5) Run the data quality snapshot
python -m gridflow.pipelines.print_dq_snapshot

# 6) Run tests
python -m pytest
```

---

## How This Maps to a Real Databricks / IEDR Setup

In a full Databricks implementation:

* Each layer (Landing, Standardized, Product, Experience) would be a set of **Delta tables**.
* Monthly utility drops would land in cloud storage and be picked up by **Autoloader** into the Bronze/Landing layer.
* **MERGE** patterns would handle updates/inserts into Silver/Gold based on key columns.
* The Experience table would likely be partitioned by date and utility for fast UI/API queries.

For **CI/CD and DQ**:

* The tests in `tests/` and the DQ snapshot would run in CI before promoting to higher environments (dev → QA → prod).
* New data quality issues (e.g., spike in nulls, orphaned join keys, abnormal value ranges) would block promotion until investigated.
* Changelogs (like the one summarized here) would sit in PRs and/or release notes so stakeholders understand what changed.

For **PII**:

* Address fields in service points would be tagged as sensitive.
* Any external-facing views or tables would expose only masked/aggregated versions.
* The raw, fully identifiable tables would be restricted to pipeline/service accounts and internal governance tooling.

---

## What This Enables Next

With the Product and Experience layers in place, the IEDR platform can:

* Drive dashboards and ad-hoc analytics directly off the **usage fact** table.
* Use the **daily summaries** for:

  * customer usage pattern analysis,
  * peak/pit detection,
  * DER siting opportunities.
* Feed clean, well-modeled data into:

  * ML models for forecasting or anomaly detection,
  * retrieval pipelines for a future chatbot over IEDR data and documentation.

The main goal of this repo is to show how I’d structure the pipelines, enforce data quality, and create layers that are simple for the IEDR app and data science teams to consume, all while making upstream data issues visible instead of sweeping them under the rug.

```

---


