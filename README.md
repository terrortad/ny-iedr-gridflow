# NY IEDR – Customer Usage Gridflow

This repo is my take on the IEDR data pipeline case study: take messy, utility-specific files and turn them into a clean, consistent model that the IEDR app (and future analytics/ML) can use without caring which utility sent the data.

---

## Problem Framing

The case provides sample data from two different NY utilities. They describe the same concepts (service points, meters, interval usage) but in very different ways:

* **Utility 1**

  * Uses `SERVICE_POINT_ID`
  * Sends interval timestamps as ISO strings
  * Includes the service point directly on the intervals

* **Utility 2**

  * Uses `premise_id` instead of service point ID
  * Sends timestamps as integers like `20250123`
  * Only gives `meter_id` on intervals; you have to join through meters to get to the service point

The IEDR application **doesn’t** want to know about any of that. It just wants:

* A **common data model** across utilities
* A **single usage table** to query for customer usage patterns
* A **summary layer** for “peak/pit usage by location and day”
* Clear signals when data is **missing, mis-joined, or out of range**

That’s what this project is meant to demonstrate

---

## Architecture Overview

I modeled this as a medallion-style pipeline, similar to how I’d implement it in Databricks:

### 1. Landing (Raw)

* Location: `data/raw/utility1`, `data/raw/utility2`
* Behavior: load CSVs as-is (no transformations), one directory per utility:

  * `utility*_service_points.csv`
  * `utility*_meters.csv`
  * `utility*_intervals.csv`

Code: [`gridflow/io_landing.py`](gridflow/io_landing.py)
This layer is about making I/O explicit and testable, not clever.

---

### 2. Standardized (Common Model)

This layer hides the utility-specific quirks and presents a **common schema** for:

* Service points
* Meters
* Interval readings

Key pieces:

#### Service points – [`build_standardized_service_points`](gridflow/io_standardized.py)

* **Utility 1**

  * `SERVICE_POINT_ID` → `service_point_id`
  * Address columns (`SERVICE_POINT_STREET`, `SERVICE_POINT_CITY`, etc.) → unified location fields
* **Utility 2**

  * `premise_id` → `service_point_id`
  * `premise_house_num`, `premise_street`, `premise_city`, `premise_zip`, `premise_region` → aligned to the same structure

**Result:** a single `service_points` table with:

* `utility_id`
* `service_point_id`
* Address/location fields
* Installed/removed/created timestamps where available

#### Meters – [`standardize_meters`](gridflow/io_standardized.py)

* **Utility 1**

  * `meter_id` normalized to string
  * No direct service point link in the file, so `service_point_id` is left null (future enhancement could infer this from additional data)
* **Utility 2**

  * `meter_id` and `meter_number` normalized
  * `premise_id` carried through as `service_point_id`
  * Installed/removed/created timestamps retained

**Result:** a single `meters` table with:

* `utility_id`
* `meter_id` (string)
* `service_point_id` (populated for Utility 2)
* Meter type, category, timestamps

#### Intervals – [`standardize_intervals`](gridflow/io_standardized.py)

* **Utility 1**

  * Carries `service_delivery_point_id` directly → `service_point_id`
  * Timestamps taken from the `timestamp` column
* **Utility 2**

  * Timestamps are integers like `20250123`
  * Explicitly parsed with `pd.to_datetime(..., format="%Y%m%d")`
  * Only has `meter_id` → we join through the standardized `meters` table to get `service_point_id`

Both utilities are then enriched with:

* `interval_start_ts`
* `interval_end_ts` (computed from duration)
* `duration_seconds`
* `value`
* `quality`
* `channel`

This is where the **common model** really comes together: regardless of which utility sends the data, downstream code now sees the same fields.

---

### 3. Product Layer – Detailed Usage

Code: [`gridflow/io_product.py`](gridflow/io_product.py), [`gridflow/pipelines/build_product.py`](gridflow/pipelines/build_product.py)

Here I join:

* Standardized service points
* Standardized meters
* Standardized intervals

into a single **usage fact** table:

**Keys:**

* `utility_id`
* `service_point_id`
* `meter_id`
* `interval_start_ts` / `interval_end_ts`

**Metrics:**

* `value` (usage)
* `duration_seconds`

**Context:**

* `channel`, `quality`
* Location fields (`city`, `zip`, `state`, etc.)
* Meter and service point metadata

This table is the “one place to go” for any usage analytics or UI queries at interval granularity.

---

### 4. Experience Layer – Daily Usage Summaries

Code: [`gridflow/io_experience.py`](gridflow/io_experience.py), [`gridflow/pipelines/build_experience.py`](gridflow/pipelines/build_experience.py)

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

---

## Data Quality & Monitoring

Data quality is a first-class concern in this design.
I added a dedicated DQ pipeline: [`gridflow/pipelines/print_dq_snapshot.py`](gridflow/pipelines/print_dq_snapshot.py)

It prints a snapshot like:

```text
[ROW COUNTS BY UTILITY]
Table                         UTILITY1     UTILITY2
--------------------------------------------------
Service Points                   1,039          195
Meters                             291          750
Intervals                          211        2,319
Product (Usage)                    211        2,319
Experience (Summary)               211        1,076

[NULL CHECKS]
  OK: No nulls in critical fields

[TIMESTAMP CHECK]
  Range: 2025-01-22 to 2025-02-21
  OK: All timestamps look reasonable

[JOIN COVERAGE]
  OK: All interval meter_ids found in meters
  ISSUE: 126 service_point_ids in intervals dont exist in service_points table

[VALUE CHECK]
  Negative values: 0
  Zero values: 85
  Range: 0.00 to 999,835.00
```

What this captures:

* **Volume by utility and by layer**
  Quickly shows what each utility is sending and where it ends up (raw → product → experience).

* **Null checks on critical fields**
  IDs and timestamps are validated before downstream consumers rely on them.

* **Timestamp sanity**
  End-to-end range check to catch parsing issues or obviously wrong dates.

* **Join coverage**
  Verifies that every interval can:

  * Join to a meter (`meter_id`),
  * Join to a service point (`service_point_id` vs `service_points`).

  The “126 orphan service points” figure is exactly the kind of thing that should trigger attention and, in production, either:

  * quarantine those rows, or
  * send a DQ alert back to the utility.

* **Value checks**

  * Negative usage (shouldn’t happen)
  * Zeros and extreme values (candidates for further rules)

In a real deployment, these metrics would be persisted and surfaced in dashboards and health checks, not just printed to the console.

---

## Bugs I Found & How I Fixed Them

Two subtle issues that the DQ + debugging surfaced:

### 1. Utility 2 Intervals Not Joining

**Symptom**

* All `service_point_id` values for Utility 2 intervals were `NaN` after the join.

**Root cause**

I originally created the Utility 2 intervals frame like this:

```python
u2_base = pd.DataFrame()
u2_base["utility_id"] = "UTILITY2"
u2_base["meter_id"] = u2_raw["meter_id"]
```

With an empty DataFrame, assigning a scalar first creates an empty column.
When `meter_id` (a Series) is added, pandas reindexes and the existing `utility_id` column becomes all nulls.

Since the join to meters uses `["utility_id", "meter_id"]`, the null `utility_id` meant “no matches” for every row.

**Fix**

Build the Utility 2 DataFrame in one go (or add `utility_id` after rows exist), and normalize meter IDs to string on both sides:

```python
u2_base = pd.DataFrame({
    "utility_id": "UTILITY2",
    "meter_id": u2_raw["meter_id"].astype(str),
    ...
})
```

Then join to the standardized meters (also with string `meter_id`) to populate `service_point_id`.

---

### 2. Timestamps Coming Out as 1970

**Symptom**

* Some timestamps were in 1970, clearly wrong for this dataset.

**Root cause**

* Utility 2 timestamps are integers like `20250123`.
  Without explicit parsing, pandas can interpret them in unintended ways.

**Fix**

Parse them explicitly with the correct format:

```python
pd.to_datetime(u2_raw["timestamp"].astype(str), format="%Y%m%d", errors="coerce")
```

The DQ snapshot showing a strange time range was the signal to look closer at these.

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
  io_standardized.py   # map U1/U2 into a common schema (SP, meters, intervals)
  io_product.py        # build detailed usage fact table
  io_experience.py     # build daily summaries with peak/pit usage
  pipelines/
    build_product.py       # pipeline entrypoint: build usage table
    build_experience.py    # pipeline entrypoint: build experience layer
    print_dq_snapshot.py   # pipeline entrypoint: run DQ snapshot

tests/
  test_service_points_unique.py   # check SP IDs are unique per utility
  test_intervals_linkage.py       # check intervals link correctly to meters/SPs
```

---

## How to Run It

Requirements: Python 3.10+ and `pandas` / `pytest`.

Install deps:

```bash
pip install pandas pytest
```

Build the detailed usage (Product) layer:

```bash
python -m gridflow.pipelines.build_product
```

Build the daily Experience layer:

```bash
python -m gridflow.pipelines.build_experience
```

Run the data quality snapshot:

```bash
python -m gridflow.pipelines.print_dq_snapshot
```

Run tests:

```bash
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

For **PII**:

* Address fields in service points would be tagged as sensitive.
* Any external-facing views or tables would expose only masked/aggregated versions.
* The raw, fully identifiable tables would be restricted to pipeline/service accounts and internal governance tooling.

---

## What This Enables Next

With the Experience and Product layers in place, the IEDR platform can:

* Drive dashboards and ad-hoc analytics directly off the **usage fact** table.
* Use the **daily summaries** for:

  * customer usage pattern analysis,
  * peak/pit detection,
  * DER siting opportunities.
* Feed clean, well-modeled data into:

  * ML models for forecasting or anomaly detection,
  * retrieval pipelines for a future chatbot over IEDR data and documentation.

The main goal of this repo is to show how I’d structure the pipelines, enforce data quality, and create layers that are simple for the IEDR app and data science teams to consume.

---
