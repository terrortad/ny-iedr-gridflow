# gridflow/io_landing.py

from pathlib import Path
import pandas as pd

# Project root = .../ny-iedr-gridflow
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def _read_csv(path: Path) -> pd.DataFrame:
    """Basic CSV reader kept in one place for consistency."""
    return pd.read_csv(path)


def load_utility1_raw() -> dict:
    """
    Load raw Utility 1 files into DataFrames.

    Returns:
      {
        "service_points": df,
        "meters": df,
        "intervals": df,
      }
    """
    u1_dir = RAW_DATA_DIR / "utility1"

    service_points_path = u1_dir / "utility1_service_points.csv"
    meters_path = u1_dir / "utility1_meters.csv"
    intervals_path = u1_dir / "utility1_intervals.csv"

    return {
        "service_points": _read_csv(service_points_path),
        "meters": _read_csv(meters_path),
        "intervals": _read_csv(intervals_path),
    }


def load_utility2_raw() -> dict:
    """
    Load raw Utility 2 files into DataFrames.

    Returns:
      {
        "service_points": df,
        "meters": df,
        "intervals": df,
      }
    """
    u2_dir = RAW_DATA_DIR / "utility2"

    service_points_path = u2_dir / "utility2_service_points.csv"
    meters_path = u2_dir / "utility2_meters.csv"
    intervals_path = u2_dir / "utility2_intervals.csv"

    return {
        "service_points": _read_csv(service_points_path),
        "meters": _read_csv(meters_path),
        "intervals": _read_csv(intervals_path),
    }


def load_all_raw() -> dict:
    """
    Convenience helper returning:

      {
        "utility1": {...},
        "utility2": {...},
      }
    """
    return {
        "utility1": load_utility1_raw(),
        "utility2": load_utility2_raw(),
    }
