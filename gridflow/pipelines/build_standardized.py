# gridflow/pipelines/build_standardized.py
"""
Pipeline script to build standardized layer from raw landing data.
Outputs summary statistics and data quality metrics.
"""

from pathlib import Path
from gridflow.io_landing import load_all_raw
from gridflow.io_standardized import (
    build_standardized_service_points,
    standardize_meters,
    standardize_intervals,
)


def main():
    print("=" * 60)
    print("BUILDING STANDARDIZED LAYER")
    print("=" * 60)

    # Load raw data
    raw = load_all_raw()

    print("\n[1/4] Loading raw data...")
    for utility, tables in raw.items():
        for table_name, df in tables.items():
            print(f"  {utility}/{table_name}: {len(df):,} rows")

    # Build standardized tables
    print("\n[2/4] Standardizing service points...")
    service_points = build_standardized_service_points(raw)
    print(f"  Output: {len(service_points):,} rows")

    print("\n[3/4] Standardizing meters...")
    meters = standardize_meters(raw)
    print(f"  Output: {len(meters):,} rows")

    print("\n[4/4] Standardizing intervals...")
    intervals = standardize_intervals(raw)
    print(f"  Output: {len(intervals):,} rows")

    # Data Quality Checks
    print("\n" + "=" * 60)
    print("DATA QUALITY SUMMARY")
    print("=" * 60)

    # Referential integrity: intervals -> service_points
    sp_ids = set(service_points["service_point_id"].dropna())
    interval_sp_ids = set(intervals["service_point_id"].dropna())
    orphan_sp_ids = interval_sp_ids - sp_ids
    orphan_interval_rows = intervals[~intervals["service_point_id"].isin(sp_ids)]

    print(f"\nReferential Integrity:")
    print(f"  Service point IDs in intervals not found in service_points: {len(orphan_sp_ids)}")
    print(f"  Affected interval rows: {len(orphan_interval_rows):,}")

    # Null checks
    print(f"\nNull Values:")
    print(f"  Intervals with null service_point_id: {intervals['service_point_id'].isna().sum():,}")
    print(f"  Meters with null service_point_id: {meters['service_point_id'].isna().sum():,}")

    # By utility breakdown
    print(f"\nBy Utility:")
    for util in ["UTILITY1", "UTILITY2"]:
        sp_count = len(service_points[service_points["utility_id"] == util])
        m_count = len(meters[meters["utility_id"] == util])
        i_count = len(intervals[intervals["utility_id"] == util])
        print(f"  {util}: {sp_count:,} SPs | {m_count:,} meters | {i_count:,} intervals")

    # Timestamp range
    print(f"\nInterval Time Range:")
    print(f"  Min: {intervals['interval_start_ts'].min()}")
    print(f"  Max: {intervals['interval_start_ts'].max()}")

    print("\n" + "=" * 60)
    print("STANDARDIZED LAYER BUILD COMPLETE")
    print("=" * 60)

    return service_points, meters, intervals


if __name__ == "__main__":
    main()