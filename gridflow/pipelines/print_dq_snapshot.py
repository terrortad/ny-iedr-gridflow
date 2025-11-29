# gridflow/pipelines/print_dq_snapshot.py
"""
Data quality snapshot for IEDR pipeline monitoring.
Run after each data refresh to catch issues early.
"""

from datetime import datetime
from gridflow.io_landing import load_all_raw
from gridflow.io_standardized import (
    build_standardized_service_points,
    standardize_meters,
    standardize_intervals,
    _normalize_columns,
)
from gridflow.io_product import build_customer_usage_interval
from gridflow.io_experience import build_customer_usage_summary
import pandas as pd


def main():
    print("IEDR Data Quality Snapshot")
    print(f"Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)

    # Load everything
    raw = load_all_raw()
    sp = build_standardized_service_points(raw)
    mt = standardize_meters(raw)
    iv = standardize_intervals(raw)
    usage = build_customer_usage_interval(sp, mt, iv)
    summary = build_customer_usage_summary(usage)

    # --- Row counts ---
    print("\nROW COUNTS")
    print(f"  {'Layer':<20} {'UTILITY1':>10} {'UTILITY2':>10} {'Total':>10}")
    for name, df in [("Service Points", sp), ("Meters", mt), ("Intervals", iv),
                     ("Product", usage), ("Experience", summary)]:
        u1 = len(df[df["utility_id"] == "UTILITY1"])
        u2 = len(df[df["utility_id"] == "UTILITY2"])
        print(f"  {name:<20} {u1:>10,} {u2:>10,} {u1+u2:>10,}")

    # --- Source data duplicates (important for understanding retention) ---
    print("\nSOURCE DATA QUALITY")
    u2_intervals = _normalize_columns(raw["utility2"]["intervals"])
    u2_raw_count = len(u2_intervals)
    u2_unique = len(u2_intervals.drop_duplicates(subset=["meter_id", "timestamp", "channel"]))
    u2_dup_pct = (1 - u2_unique / u2_raw_count) * 100

    print(f"  UTILITY2 interval duplicates: {u2_raw_count - u2_unique:,} ({u2_dup_pct:.0f}% of raw)")
    print(f"  These are same meter/timestamp/channel with different values.")

    # --- Referential integrity ---
    print("\nREFERENTIAL INTEGRITY")
    sp_ids = set(sp["service_point_id"].dropna())
    mt_ids = set(mt["meter_id"].dropna())

    # intervals -> meters
    iv_meter_ids = set(iv["meter_id"].dropna())
    orphan_meters = iv_meter_ids - mt_ids
    print(f"  Intervals -> Meters:         {len(orphan_meters)} orphan meter_ids", end="")
    print(" ✓" if len(orphan_meters) == 0 else " ⚠")

    # intervals -> service points
    iv_sp_ids = set(iv["service_point_id"].dropna())
    orphan_sps = iv_sp_ids - sp_ids
    orphan_rows = len(iv[~iv["service_point_id"].isin(sp_ids)])
    print(f"  Intervals -> Service Points: {len(orphan_sps)} orphan SP IDs ({orphan_rows:,} rows)", end="")
    print(" ✓" if len(orphan_sps) == 0 else " ⚠")

    # Break down by utility
    for util in ["UTILITY1", "UTILITY2"]:
        util_iv = iv[iv["utility_id"] == util]
        util_sp_ids = set(sp[sp["utility_id"] == util]["service_point_id"])
        util_orphans = set(util_iv["service_point_id"].dropna()) - util_sp_ids
        if util_orphans:
            print(f"    {util}: {len(util_orphans)} missing premises")

    # --- Completeness ---
    print("\nCOMPLETENESS (% non-null)")
    critical_fields = {
        "Intervals": (iv, ["service_point_id", "meter_id", "interval_start_ts", "value"]),
        "Meters": (mt, ["meter_id", "service_point_id"]),
        "Service Points": (sp, ["service_point_id", "city", "zip"]),
    }
    for table_name, (df, cols) in critical_fields.items():
        missing = []
        for col in cols:
            if col in df.columns:
                pct = df[col].notna().mean() * 100
                if pct < 100:
                    missing.append(f"{col}:{pct:.0f}%")
        if missing:
            print(f"  {table_name}: {', '.join(missing)}")
        else:
            print(f"  {table_name}: all critical fields complete")

    # --- Value checks ---
    print("\nINTERVAL VALUES")
    vals = iv["value"]
    print(f"  Range: {vals.min():,.0f} to {vals.max():,.0f}")
    print(f"  Mean: {vals.mean():,.0f}, Median: {vals.median():,.0f}")
    print(f"  Zeros: {(vals == 0).sum():,} ({(vals == 0).mean()*100:.1f}%)")
    print(f"  Negatives: {(vals < 0).sum()}")

    # Outlier check (simple IQR method)
    q1, q3 = vals.quantile([0.25, 0.75])
    iqr = q3 - q1
    outliers = ((vals < q1 - 1.5*iqr) | (vals > q3 + 1.5*iqr)).sum()
    print(f"  Outliers (IQR): {outliers:,} ({outliers/len(vals)*100:.1f}%)")

    # --- Timestamps ---
    print("\nTIMESTAMP RANGE")
    ts = pd.to_datetime(iv["interval_start_ts"], errors="coerce", utc=True)
    print(f"  {ts.min().date()} to {ts.max().date()} ({(ts.max() - ts.min()).days} days)")
    print(f"  Nulls: {ts.isna().sum()}, Pre-2020: {(ts < '2020-01-01').sum()}")

    # --- Experience layer sanity ---
    print("\nEXPERIENCE LAYER")
    if not summary.empty:
        print(f"  {len(summary):,} daily summaries for {summary['service_point_id'].nunique()} service points")
        print(f"  Avg daily usage: {summary['total_usage'].mean():,.0f}")
        print(f"  Peak value seen: {summary['peak_usage_value'].max():,.0f}")
    else:
        print("  WARNING: Empty - check joins")

    # --- Overall health ---
    print("\n" + "-" * 50)
    print("SUMMARY")

    issues = []
    if orphan_sps:
        issues.append(f"{len(orphan_sps)} SP IDs missing from UTILITY2 (source issue)")
    if u2_dup_pct > 50:
        issues.append(f"UTILITY2 has {u2_dup_pct:.0f}% duplicate readings (clarify with utility)")
    if (vals < 0).sum() > 0:
        issues.append("Negative usage values found")

    linkage_rate = (len(iv) - orphan_rows) / len(iv) * 100
    print(f"  Records ready for analysis: {linkage_rate:.0f}% ({len(iv) - orphan_rows:,} / {len(iv):,})")

    if issues:
        print(f"  Issues to address:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print("  No blocking issues found")

    print("-" * 50)


if __name__ == "__main__":
    main()