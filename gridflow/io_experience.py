# gridflow/io_experience.py

import pandas as pd


def build_customer_usage_summary(
    customer_usage_interval: pd.DataFrame,
    window: str = "D",
) -> pd.DataFrame:
    """
    Experience (Platinum) layer summarizing usage per service point
    over a time window (default: daily).

    Handles mixed timestamp formats and missing IDs by:
      - coercing invalid timestamps to NaT and dropping those rows
      - filling missing utility_id/service_point_id with explicit markers
    """

    df = customer_usage_interval.copy()

    # Make sure IDs are usable for grouping
    df["utility_id"] = df["utility_id"].fillna("UNKNOWN_UTILITY")
    df["service_point_id"] = df["service_point_id"].fillna("UNKNOWN_SERVICE_POINT")

    # Robust timestamp parsing, allow mixed formats
    df["interval_start_ts"] = pd.to_datetime(
        df["interval_start_ts"].astype(str),
        errors="coerce",
        utc=True,
    )
    df = df[df["interval_start_ts"].notna()].copy()

    if df.empty:
        return pd.DataFrame(
            columns=[
                "utility_id",
                "service_point_id",
                "bucket_start",
                "bucket_end",
                "total_usage",
                "interval_count",
                "peak_usage_value",
                "peak_usage_ts",
                "pit_usage_value",
                "pit_usage_ts",
            ]
        )

    # Drop timezone before converting to Period to avoid warnings
    df["interval_start_ts"] = df["interval_start_ts"].dt.tz_convert(None)

    periods = df["interval_start_ts"].dt.to_period(window)
    df["bucket_start"] = periods.dt.start_time
    df["bucket_end"] = periods.dt.end_time

    grouping_cols = ["utility_id", "service_point_id", "bucket_start", "bucket_end"]

    # Total usage per bucket
    agg_total = (
        df.groupby(grouping_cols)
        .agg(
            total_usage=("value", "sum"),
            interval_count=("value", "count"),
        )
        .reset_index()
    )

    # Peak usage per bucket
    peak_idx = df.groupby(grouping_cols)["value"].idxmax()
    peak = df.loc[peak_idx, grouping_cols + ["value", "interval_start_ts"]].rename(
        columns={
            "value": "peak_usage_value",
            "interval_start_ts": "peak_usage_ts",
        }
    )

    # Lowest usage per bucket
    pit_idx = df.groupby(grouping_cols)["value"].idxmin()
    pit = df.loc[pit_idx, grouping_cols + ["value", "interval_start_ts"]].rename(
        columns={
            "value": "pit_usage_value",
            "interval_start_ts": "pit_usage_ts",
        }
    )

    summary = (
        agg_total
        .merge(peak, on=grouping_cols, how="left")
        .merge(pit, on=grouping_cols, how="left")
    )

    return summary
