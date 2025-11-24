# gridflow/io_product.py

import pandas as pd


def build_customer_usage_interval(
    standardized_service_points: pd.DataFrame,
    standardized_meters: pd.DataFrame,
    standardized_intervals: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join standardized service points, meters, and intervals into a single
    detailed usage table (Product layer).
    """

    sp = standardized_service_points.copy()
    mt = standardized_meters.copy()
    iv = standardized_intervals.copy()

    # Join intervals -> meters on utility + meter id
    iv_mt = iv.merge(
        mt,
        on=["utility_id", "meter_id"],
        how="left",
        suffixes=("", "_meter"),
    )

    # Join that result -> service points on utility + service point id
    iv_mt_sp = iv_mt.merge(
        sp,
        on=["utility_id", "service_point_id"],
        how="left",
        suffixes=("", "_sp"),
    )

    # Ensure utility_id is always populated
    iv_mt_sp["utility_id"] = iv_mt_sp["utility_id"].fillna("UNKNOWN_UTILITY")

    base_cols = [
        "utility_id",
        "service_point_id",
        "meter_id",
        "interval_start_ts",
        "interval_end_ts",
        "duration_seconds",
        "value",
        "channel",
        "quality",
    ]

    location_cols = ["city", "zip", "state"]

    existing_cols = list(iv_mt_sp.columns)
    extra_cols = [c for c in existing_cols if c not in base_cols + location_cols]

    ordered_cols = [c for c in base_cols + location_cols + extra_cols if c in existing_cols]
    usage = iv_mt_sp[ordered_cols]

    return usage
