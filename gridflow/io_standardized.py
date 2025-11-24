# gridflow/io_standardized.py

from typing import Dict
import pandas as pd


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and strip column names to keep mappings consistent."""
    out = df.copy()
    out.columns = [c.strip().lower() for c in out.columns]
    return out


# ---------------------------
# Service points (Utility 1 & 2)
# ---------------------------

def standardize_utility1_service_points(raw_sp: pd.DataFrame) -> pd.DataFrame:
    """
    Map utility1_service_points.csv into the common service_point model.

    Utility 1 columns:
      SERVICE_POINT_ID, SERVICE_POINT_NUMBER, SERVICE_POINT_STREET,
      SERVICE_POINT_CITY, SERVICE_POINT_ZIP, SERVICE_POINT_STATE,
      INSTALLED_AT, REMOVED_AT, CREATED, UPDATED
    """
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
    """
    Map utility2_service_points.csv into the common service_point model.

    Utility 2 columns:
      premise_id, created_date, premise_house_num, premise_street,
      premise_house_supp, premise_city, premise_zip, premise_region
    """
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
    """Combined standardized service_point table across utilities."""
    u1_sp = standardize_utility1_service_points(raw_all["utility1"]["service_points"])
    u2_sp = standardize_utility2_service_points(raw_all["utility2"]["service_points"])

    combined = pd.concat([u1_sp, u2_sp], ignore_index=True)
    combined = combined.drop_duplicates(subset=["utility_id", "service_point_id"])

    # As a safety net, if utility_id is ever missing, infer from ID pattern
    combined["utility_id"] = combined["utility_id"].fillna(
        combined["service_point_id"].astype(str).apply(
            lambda sp: "UTILITY1" if sp.startswith("SP-") else "UTILITY2"
        )
    )

    return combined


# ---------------------------
# Meters (Utility 1 & 2)
# ---------------------------

def standardize_meters(
    raw_all: Dict[str, Dict[str, pd.DataFrame]]
) -> pd.DataFrame:
    """
    Standardize meter metadata.

    Utility 1 meters (utility1_meters.csv):
      meter_id, meter_timestamp, meter_duration, meter_value,
      meter_type, meter_category

    Utility 2 meters (utility2_meters.csv):
      premise_id, meter_id, meter_number, meter_type,
      meter_status, meter_channel, installed_at, removed_at,
      created, updated
    """
    # Utility 1
    u1_raw = _normalize_columns(raw_all["utility1"]["meters"])
    u1 = pd.DataFrame()
    u1["utility_id"] = "UTILITY1"
    u1["meter_id"] = u1_raw["meter_id"]
    u1["serial_number"] = u1_raw["meter_id"]
    u1["meter_type"] = u1_raw.get("meter_type")
    u1["meter_category"] = u1_raw.get("meter_category")
    # No direct mapping to service point in this file
    u1["service_point_id"] = None
    u1["installed_at"] = None
    u1["removed_at"] = None
    u1["created_at"] = None
    u1["updated_at"] = None

    # Utility 2
    u2_raw = _normalize_columns(raw_all["utility2"]["meters"])
    u2 = pd.DataFrame()
    u2["utility_id"] = "UTILITY2"
    u2["meter_id"] = u2_raw["meter_id"]
    u2["serial_number"] = u2_raw["meter_number"]
    u2["meter_type"] = u2_raw["meter_type"]
    u2["meter_category"] = u2_raw.get("meter_channel")
    u2["service_point_id"] = u2_raw["premise_id"]
    u2["installed_at"] = u2_raw.get("installed_at")
    u2["removed_at"] = u2_raw.get("removed_at")
    u2["created_at"] = u2_raw.get("created")
    u2["updated_at"] = u2_raw.get("updated")

    combined = pd.concat([u1, u2], ignore_index=True)
    combined = combined.drop_duplicates(subset=["utility_id", "meter_id"])

    # Safety net for utility_id
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
# Intervals (Utility 1 & 2)
# ---------------------------

def _add_interval_end(df: pd.DataFrame) -> pd.DataFrame:
    """Given interval_start_ts + duration_seconds, compute interval_end_ts."""
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
    Standardize interval readings.

    Utility 1 intervals (utility1_intervals.csv):
      service_delivery_point_id, meter_id, channel, duration, value,
      quality, timestamp, last_update_time, exported_at

    Utility 2 intervals (utility2_intervals.csv):
      channel, duration, meter_id, quality, timestamp, value

    For Utility 2, service_point_id is derived via meters (premise_id).
    """
    meters_std = standardize_meters(raw_all)

    # Utility 1
    u1_raw = _normalize_columns(raw_all["utility1"]["intervals"])
    u1 = pd.DataFrame()
    u1["utility_id"] = "UTILITY1"
    u1["service_point_id"] = u1_raw["service_delivery_point_id"]
    u1["meter_id"] = u1_raw["meter_id"]
    u1["interval_start_ts"] = u1_raw["timestamp"]
    u1["duration_seconds"] = pd.to_numeric(u1_raw["duration"], errors="coerce")
    u1["value"] = pd.to_numeric(u1_raw["value"], errors="coerce")
    u1["quality"] = u1_raw["quality"]
    u1["channel"] = u1_raw["channel"]
    u1["last_update_time"] = u1_raw.get("last_update_time")
    u1["exported_at"] = u1_raw.get("exported_at")

    u1 = _add_interval_end(u1)

    # Utility 2
    u2_raw = _normalize_columns(raw_all["utility2"]["intervals"])
    u2_base = pd.DataFrame()
    u2_base["utility_id"] = "UTILITY2"
    u2_base["meter_id"] = u2_raw["meter_id"]
    u2_base["interval_start_ts"] = u2_raw["timestamp"]
    u2_base["duration_seconds"] = pd.to_numeric(u2_raw["duration"], errors="coerce")
    u2_base["value"] = pd.to_numeric(u2_raw["value"], errors="coerce")
    u2_base["quality"] = u2_raw["quality"]
    u2_base["channel"] = u2_raw["channel"]
    u2_base["last_update_time"] = None
    u2_base["exported_at"] = None

    # Attach service_point_id via standardized meters (utility2)
    meters_u2 = meters_std[meters_std["utility_id"] == "UTILITY2"][
        ["utility_id", "meter_id", "service_point_id"]
    ]
    u2 = u2_base.merge(
        meters_u2,
        on=["utility_id", "meter_id"],
        how="left",
    )

    u2 = _add_interval_end(u2)

    combined = pd.concat([u1, u2], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["utility_id", "service_point_id", "meter_id", "interval_start_ts"]
    )

    # Safety net for utility_id
    combined["utility_id"] = combined["utility_id"].fillna(
        combined["service_point_id"]
        .combine_first(combined["meter_id"])
        .astype(str)
        .apply(
            lambda x: "UTILITY1" if x.startswith("SP-") or x.startswith("MTR-") else "UTILITY2"
        )
    )

    return combined
