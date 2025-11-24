from gridflow.io_landing import load_all_raw
from gridflow.io_standardized import build_standardized_service_points


def test_service_points_unique_per_utility():
    raw = load_all_raw()
    sp = build_standardized_service_points(raw)

    # No null IDs
    assert sp["service_point_id"].notnull().all()

    # Unique per (utility_id, service_point_id)
    grouped = sp.groupby(["utility_id", "service_point_id"]).size()
    assert (grouped <= 1).all()
