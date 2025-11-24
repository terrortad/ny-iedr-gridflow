# gridflow/pipelines/print_dq_snapshot.py

from gridflow.io_landing import load_all_raw
from gridflow.io_standardized import (
    build_standardized_service_points,
    standardize_meters,
    standardize_intervals,
)
from gridflow.io_product import build_customer_usage_interval
from gridflow.io_experience import build_customer_usage_summary
import pandas as pd


def main():
    raw_all = load_all_raw()

    sp = build_standardized_service_points(raw_all)
    mt = standardize_meters(raw_all)
    iv = standardize_intervals(raw_all)
    usage = build_customer_usage_interval(sp, mt, iv)
    summary = build_customer_usage_summary(usage)

    print("=" * 60)
    print("IEDR DATA QUALITY SNAPSHOT")
    print("=" * 60)

    # Row counts by utility
    print("\n[ROW COUNTS BY UTILITY]")
    print(f"{'Table':<25} {'UTILITY1':>12} {'UTILITY2':>12}")
    print("-" * 50)
    for name, df in [("Service Points", sp), ("Meters", mt), ("Intervals", iv), ("Product (Usage)", usage), ("Experience (Summary)", summary)]:
        u1 = len(df[df["utility_id"] == "UTILITY1"])
        u2 = len(df[df["utility_id"] == "UTILITY2"])
        print(f"{name:<25} {u1:>12,} {u2:>12,}")

    # Null checks on fields that should never be null
    print("\n[NULL CHECKS]")
    checks = [
        ("Intervals", iv, ["utility_id", "meter_id", "service_point_id", "interval_start_ts", "value"]),
        ("Meters", mt, ["utility_id", "meter_id"]),
        ("Service Points", sp, ["utility_id", "service_point_id"]),
    ]
    issues_found = False
    for table_name, df, cols in checks:
        for col in cols:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    print(f"  ISSUE: {table_name}.{col} has {null_count:,} nulls")
                    issues_found = True
    if not issues_found:
        print("  OK: No nulls in critical fields")

    # Timestamp sanity - catches bad parsing like epoch dates
    print("\n[TIMESTAMP CHECK]")
    if "interval_start_ts" in iv.columns:
        ts = pd.to_datetime(iv["interval_start_ts"], errors="coerce")
        min_ts = ts.min()
        max_ts = ts.max()
        print(f"  Range: {min_ts.date()} to {max_ts.date()}")
        
        old_timestamps = ts[ts < "2020-01-01"].count()
        future_timestamps = ts[ts > "2030-01-01"].count()
        if old_timestamps > 0:
            print(f"  ISSUE: {old_timestamps:,} timestamps before 2020 - likely a parsing problem")
        if future_timestamps > 0:
            print(f"  ISSUE: {future_timestamps:,} timestamps after 2030 - check source format")
        if old_timestamps == 0 and future_timestamps == 0:
            print("  OK: All timestamps look reasonable")

    # Check that intervals can actually join to meters and service points
    print("\n[JOIN COVERAGE]")
    interval_meters = set(zip(iv["utility_id"], iv["meter_id"]))
    known_meters = set(zip(mt["utility_id"], mt["meter_id"]))
    orphan_intervals = interval_meters - known_meters
    if orphan_intervals:
        print(f"  ISSUE: {len(orphan_intervals)} meter_ids in intervals dont exist in meters table")
    else:
        print("  OK: All interval meter_ids found in meters")

    interval_sps = set(iv[iv["service_point_id"].notna()][["utility_id", "service_point_id"]].itertuples(index=False, name=None))
    known_sps = set(zip(sp["utility_id"], sp["service_point_id"]))
    orphan_sps = interval_sps - known_sps
    if orphan_sps:
        print(f"  ISSUE: {len(orphan_sps)} service_point_ids in intervals dont exist in service_points table")
    else:
        print("  OK: All interval service_point_ids found in service_points")

    # Basic sanity on the actual usage values
    print("\n[VALUE CHECK]")
    if "value" in iv.columns:
        val = pd.to_numeric(iv["value"], errors="coerce")
        negative_count = (val < 0).sum()
        zero_count = (val == 0).sum()
        print(f"  Negative values: {negative_count:,}")
        print(f"  Zero values: {zero_count:,}")
        print(f"  Range: {val.min():,.2f} to {val.max():,.2f}")

    # Final output layer
    print("\n[EXPERIENCE LAYER]")
    if not summary.empty:
        print(f"  Total daily summaries: {len(summary):,}")
        print(f"  Date range: {summary['bucket_start'].min().date()} to {summary['bucket_end'].max().date()}")
        print(f"  Avg intervals per day: {summary['interval_count'].mean():.1f}")
        print(f"  Peak usage max: {summary['peak_usage_value'].max():,.2f}")
    else:
        print("  ISSUE: Experience summary is empty - check upstream joins")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()