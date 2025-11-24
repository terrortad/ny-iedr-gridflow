# NY IEDR – Customer Usage Gridflow

This repo is my approach to the IEDR data pipeline case study. The goal was to take utility-specific files and normalize them into a clean, consistent model that the IEDR app can query without caring which utility sent the data.

---

## The Problem

The case study provides data from two utilities, and they send their data in completely different formats:

**Utility 1**
- Uses `SERVICE_POINT_ID` with all-caps column names
- Sends timestamps as ISO strings
- Includes the service point directly on interval records

**Utility 2**
- Uses `premise_id` instead of service point
- Sends timestamps as integers like `20250123`
- Only provides `meter_id` on intervals, so you have to join through meters to get to the service point

The IEDR application needs a common data model across all utilities, a single usage table for customer usage patterns, daily summaries with peak and pit usage by location, and clear visibility into missing or malformed data.

---

## Pipeline Design

I structured this as a medallion pipeline, similar to how it would be built in Databricks:

### Landing (Raw)

Raw files from each utility sit in `data/raw/utility1/` and `data/raw/utility2/`. No transformations happen here - just loading the CSVs exactly as they arrived.

Code: `gridflow/io_landing.py`

### Standardized (Common Model)

This layer handles the schema differences between utilities:

**Service Points**

Utility 1's `SERVICE_POINT_ID` and Utility 2's `premise_id` both map to a common `service_point_id` field. The address fields get normalized into common columns like `street`, `city`, `zip`, and `state`.

**Meters**

Both utilities get normalized to a common schema. Utility 2 meters link to `premise_id`, which provides the service point connection. Utility 1 meters dont have that link in this dataset.

**Intervals**

For Utility 1, `service_delivery_point_id` maps directly to service point. For Utility 2, only `meter_id` is available, so the join goes through the meters table to get the service point. Utility 2 timestamps also needed parsing from their `YYYYMMDD` integer format. After standardization, both utilities have the same fields: `interval_start_ts`, `interval_end_ts`, `duration_seconds`, `value`, `quality`, and `channel`.

Code: `gridflow/io_standardized.py`

### Product (Joined Usage Table)

This layer joins standardized service points, meters, and intervals into one usage fact table. Each row represents one interval reading with all the meter and location context attached. Downstream reporting or APIs would query this table for detailed usage data.

Code: `gridflow/io_product.py`

### Experience (Daily Summaries)

The product table gets aggregated into daily buckets per service point. For each day and service point, the output includes `total_usage`, `interval_count`, `peak_usage_value` with its timestamp, and `pit_usage_value` with its timestamp. This layer is designed for a single API call - the UI can request usage patterns for a service point over time without doing any aggregation itself.

Code: `gridflow/io_experience.py`

---

## Data Quality

Data quality checks are built into the pipeline. The DQ snapshot validates each layer before data moves downstream.

Current output:
```
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

[EXPERIENCE LAYER]
  Total daily summaries: 1,287
  Date range: 2025-01-22 to 2025-02-21
  Avg intervals per day: 2.0
  Peak usage max: 999,835.00
```

What this catches:
- **Null checks** - IDs and timestamps that should never be empty
- **Timestamp sanity** - Parsing issues like epoch dates from 1970
- **Join coverage** - Intervals that cant link to meters or service points
- **Value ranges** - Negative usage, zeros, outliers

The 126 orphan service points are a real data quality finding. In production, those would either be quarantined or trigger an alert back to the utility.

Code: `gridflow/pipelines/print_dq_snapshot.py`

---

## Issues Found During Development

Two issues came up that the DQ checks helped surface:

### 1. Utility 2 Intervals Not Joining

All Utility 2 intervals had null `service_point_id` after the join to meters.

**What happened**: When building the DataFrame, assigning `utility_id = "UTILITY2"` to an empty DataFrame first created an empty column. Adding the `meter_id` series after that caused pandas to add rows but leave `utility_id` as null for all of them. Since the join uses both columns, nothing matched.

**Fix**: Building the DataFrame with a dictionary constructor broadcasts the scalar value to all rows:
```python
u2_base = pd.DataFrame({
    "utility_id": "UTILITY2",
    "meter_id": u2_raw["meter_id"].astype(str),
    ...
})
```

### 2. Timestamps Showing 1970 Dates

Some timestamps were coming through as 1970 dates.

**What happened**: Utility 2 timestamps are integers like `20250123`. Without explicit format parsing, pandas misinterpreted them as epoch values.

**Fix**: Parsing with the correct format:
```python
pd.to_datetime(u2_raw["timestamp"].astype(str), format="%Y%m%d", errors="coerce")
```

The DQ timestamp check flagged dates outside the expected range, which led to finding this issue.

---

## Project Structure
```
data/raw/
  utility1/
    utility1_service_points.csv
    utility1_meters.csv
    utility1_intervals.csv
  utility2/
    utility2_service_points.csv
    utility2_meters.csv
    utility2_intervals.csv

gridflow/
  io_landing.py          # load raw files
  io_standardized.py     # normalize to common schema
  io_product.py          # build joined usage table
  io_experience.py       # build daily summaries
  pipelines/
    build_product.py
    build_experience.py
    print_dq_snapshot.py

tests/
  test_service_points_unique.py
  test_intervals_linkage.py
```

---

## How to Run

Requires Python 3.10+ and pandas.
```bash
pip install pandas pytest
```

Build the joined usage table:
```bash
python -m gridflow.pipelines.build_product
```

Build daily summaries:
```bash
python -m gridflow.pipelines.build_experience
```

Run data quality checks:
```bash
python -m gridflow.pipelines.print_dq_snapshot
```

Run tests:
```bash
python -m pytest
```

---

## Production Considerations

In a Databricks deployment:
- Each layer would be a Delta table
- Monthly utility drops would come through autoloader
- MERGE statements would handle updates keyed on `(utility_id, service_point_id, meter_id, interval_start_ts)`
- The Experience table would be partitioned by date for query performance

For CI/CD:
- Tests and DQ checks would run before promoting code between environments
- New data quality issues would block deployment until investigated

For PII:
- Address fields would be tagged as sensitive
- External-facing views would only expose masked or aggregated data
- Raw tables would be restricted to pipeline service accounts

---

## What This Enables

With the Product and Experience layers in place, the IEDR platform can:
- Query detailed usage data without knowing which utility it came from
- Pull daily summaries for dashboards and usage pattern analysis
- Identify peak and pit usage for DER siting opportunities
- Feed clean, well-modeled data into ML models or retrieval systems
