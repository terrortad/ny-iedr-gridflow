# gridflow/pipelines/print_dq_snapshot.py

from gridflow.io_landing import load_all_raw
from gridflow.io_standardized import (
    build_standardized_service_points,
    standardize_meters,
    standardize_intervals,
)
from gridflow.io_product import build_customer_usage_interval
from gridflow.io_experience import build_customer_usage_summary


def main():
    raw_all = load_all_raw()

    sp = build_standardized_service_points(raw_all)
    mt = standardize_meters(raw_all)
    iv = standardize_intervals(raw_all)
    usage = build_customer_usage_interval(sp, mt, iv)
    summary = build_customer_usage_summary(usage)

    print("=== SERVICE POINTS ===")
    print(sp["utility_id"].value_counts(dropna=False))
    print()

    print("=== METERS ===")
    print(mt["utility_id"].value_counts(dropna=False))
    print()

    print("=== INTERVALS ===")
    print(iv["utility_id"].value_counts(dropna=False))
    print("Intervals missing service_point_id:", iv["service_point_id"].isna().sum())
    print()

    print("=== EXPERIENCE SUMMARY (DAILY) ===")
    if not summary.empty:
        print(summary["utility_id"].value_counts(dropna=False))
        print(
            "Date range:",
            summary["bucket_start"].min(),
            "→",
            summary["bucket_end"].max(),
        )
        print("Daily interval_count stats:")
        print(summary["interval_count"].describe())
    else:
        print("No rows in summary table")


if __name__ == "__main__":
    main()
