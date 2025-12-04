# gridflow/io_standardized.py

from typing import Dict
import pandas as pd


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and strip whitespace from column names."""
    out = df.copy()
    out.columns = [c.strip().lower() for c in out.columns]
    return out


# ---------------------------
# Service Points
# ---------------------------

def standardize_utility1_service_points(raw_sp: pd.DataFrame) -> pd.DataFrame:
    """Utility 1 uses SERVICE_POINT_ID and all-caps column names."""
    df = _normalize_columns(raw_sp)

    standardized = pd.DataFrame()
    standardized["utility_id"] = "UTILITY1"
    standardized["service_point_id"] = df["service_point_id"]
    standardized["service_point_number"] = df["service_point_number"]
    standardized["house_num"] = None
    standardized["street"] = df["service_point_street"]
    standardized["house_supp"] = None
    standardized["city"] = df["service_point_city"]
    standardized["zip"] = df["service_point_zip"]
    standardized["state"] = df["service_point_state"]
    standardized["installed_at"] = df.get("installed_at")
    standardized["removed_at"] = df.get("removed_at")
    standardized["created_at"] = df.get("created")
    standardized["updated_at"] = df.get("updated")

    return standardized


def standardize_utility2_service_points(raw_sp: pd.DataFrame) -> pd.DataFrame:
    """Utility 2 uses premise_id instead of service_point_id."""
    df = _normalize_columns(raw_sp)

    standardized = pd.DataFrame()
    standardized["utility_id"] = "UTILITY2"
    standardized["service_point_id"] = df["premise_id"]
    standardized["service_point_number"] = None
    standardized["house_num"] = df["premise_house_num"]
    standardized["street"] = df["premise_street"]
    standardized["house_supp"] = df.get("premise_house_supp")
    standardized["city"] = df["premise_city"]
    standardized["zip"] = df["premise_zip"]
    standardized["state"] = df["premise_region"]
    standardized["installed_at"] = None
    standardized["removed_at"] = None
    standardized["created_at"] = df.get("created_date")
    standardized["updated_at"] = None

    return standardized


def build_standardized_service_points(
    raw_all: Dict[str, Dict[str, pd.DataFrame]]
) -> pd.DataFrame:
    """Combine service points from both utilities into one table."""
    u1_sp = standardize_utility1_service_points(raw_all["utility1"]["service_points"])
    u2_sp = standardize_utility2_service_points(raw_all["utility2"]["service_points"])

    combined = pd.concat([u1_sp, u2_sp], ignore_index=True)

    # NOTE: Current implementation does full refresh for prototype.
    # Production would use Delta MERGE for incremental monthly updates:
    #   MERGE INTO silver.service_points USING staging
    #   ON target.utility_id = source.utility_id 
    #      AND target.service_point_id = source.service_point_id
    #   WHEN MATCHED THEN UPDATE SET *
    #   WHEN NOT MATCHED THEN INSERT *
    
    combined = combined.drop_duplicates(subset=["utility_id", "service_point_id"])

    # Fallback if utility_id is missing - infer from ID format
    combined["utility_id"] = combined["utility_id"].fillna(
        combined["service_point_id"].astype(str).apply(
            lambda sp: "UTILITY1" if sp.startswith("SP-") else "UTILITY2"
        )
    )

    return combined


# ---------------------------
# Meters
# ---------------------------

def standardize_meters(
    raw_all: Dict[str, Dict[str, pd.DataFrame]]
) -> pd.DataFrame:
    """
    Combine meters from both utilities.
    Utility 2 meters link to premise_id (service point). Utility 1 meters dont have that link.
    """
    # Utility 1
    u1_raw = _normalize_columns(raw_all["utility1"]["meters"])
    u1 = pd.DataFrame({
        "utility_id": "UTILITY1",
        "meter_id": u1_raw["meter_id"].astype(str),
        "serial_number": u1_raw["meter_id"].astype(str),
        "meter_type": u1_raw.get("meter_type"),
        "meter_category": u1_raw.get("meter_category"),
        "service_point_id": None,
        "installed_at": None,
        "removed_at": None,
        "created_at": None,
        "updated_at": None,
    })

    # Utility 2
    u2_raw = _normalize_columns(raw_all["utility2"]["meters"])
    u2 = pd.DataFrame({
        "utility_id": "UTILITY2",
        "meter_id": u2_raw["meter_id"].astype(str),
        "serial_number": u2_raw["meter_number"].astype(str),
        "meter_type": u2_raw["meter_type"],
        "meter_category": u2_raw.get("meter_channel"),
        "service_point_id": u2_raw["premise_id"],
        "installed_at": u2_raw.get("installed_at"),
        "removed_at": u2_raw.get("removed_at"),
        "created_at": u2_raw.get("created"),
        "updated_at": u2_raw.get("updated"),
    })

    combined = pd.concat([u1, u2], ignore_index=True)
    combined = combined.drop_duplicates(subset=["utility_id", "meter_id"])

    # Fallback if utility_id is missing
    combined["utility_id"] = combined["utility_id"].fillna(
        combined["service_point_id"]
        .combine_first(combined["meter_id"])
        .astype(str)
        .apply(
            lambda x: "UTILITY1" if x.startswith("SP-") or x.startswith("MTR-") else "UTILITY2"
        )
    )

    return combined


# ---------------------------
# Intervals
# ---------------------------

def _add_interval_end(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate interval_end_ts from start time + duration."""
    out = df.copy()
    start = pd.to_datetime(out["interval_start_ts"], errors="coerce", utc=True)
    duration = pd.to_numeric(out["duration_seconds"], errors="coerce").fillna(0)
    out["interval_start_ts"] = start
    out["interval_end_ts"] = start + pd.to_timedelta(duration, unit="s")
    return out


def standardize_intervals(
    raw_all: Dict[str, Dict[str, pd.DataFrame]]
) -> pd.DataFrame:
    """
    Combine interval readings from both utilities.
    
    Utility 1 includes service_delivery_point_id directly.
    Utility 2 only has meter_id - we join through meters to get service_point_id.
    Utility 2 timestamps are YYYYMMDD integers, not ISO strings.
    """
    meters_std = standardize_meters(raw_all)

    # Utility 1
    u1_raw = _normalize_columns(raw_all["utility1"]["intervals"])
    u1 = pd.DataFrame({
        "utility_id": "UTILITY1",
        "service_point_id": u1_raw["service_delivery_point_id"],
        "meter_id": u1_raw["meter_id"].astype(str),
        "interval_start_ts": u1_raw["timestamp"],
        "duration_seconds": pd.to_numeric(u1_raw["duration"], errors="coerce"),
        "value": pd.to_numeric(u1_raw["value"], errors="coerce"),
        "quality": u1_raw["quality"],
        "channel": u1_raw["channel"],
        "last_update_time": u1_raw.get("last_update_time"),
        "exported_at": u1_raw.get("exported_at"),
    })
    u1 = _add_interval_end(u1)

    # Utility 2 - timestamps are YYYYMMDD integers
    u2_raw = _normalize_columns(raw_all["utility2"]["intervals"])
    u2_base = pd.DataFrame({
        "utility_id": "UTILITY2",
        "meter_id": u2_raw["meter_id"].astype(str),
        "interval_start_ts": pd.to_datetime(u2_raw["timestamp"].astype(str), format="%Y%m%d", errors="coerce"),
        "duration_seconds": pd.to_numeric(u2_raw["duration"], errors="coerce"),
        "value": pd.to_numeric(u2_raw["value"], errors="coerce"),
        "quality": u2_raw["quality"],
        "channel": u2_raw["channel"],
        "last_update_time": None,
        "exported_at": None,
    })

    # Get service_point_id by joining to meters
    meters_u2 = meters_std[meters_std["utility_id"] == "UTILITY2"][
        ["utility_id", "meter_id", "service_point_id"]
    ].copy()
    meters_u2["meter_id"] = meters_u2["meter_id"].astype(str)

    u2 = u2_base.merge(
        meters_u2,
        on=["utility_id", "meter_id"],
        how="left",
    )
    u2 = _add_interval_end(u2)

    combined = pd.concat([u1, u2], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["utility_id", "service_point_id", "meter_id", "interval_start_ts", "channel"]
    )

    # Fallback if utility_id is missing
    combined["utility_id"] = combined["utility_id"].fillna(
        combined["service_point_id"]
        .combine_first(combined["meter_id"])
        .astype(str)
        .apply(
            lambda x: "UTILITY1" if x.startswith("SP-") or x.startswith("MTR-") else "UTILITY2"
        )
    )

    return combined