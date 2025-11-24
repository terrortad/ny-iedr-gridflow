from gridflow.io_landing import load_all_raw
from gridflow.io_standardized import standardize_meters, standardize_intervals


def test_intervals_have_meters():
    raw = load_all_raw()
    meters = standardize_meters(raw)
    intervals = standardize_intervals(raw)

    meter_keys = set(zip(meters["utility_id"], meters["meter_id"]))
    interval_keys = set(zip(intervals["utility_id"], intervals["meter_id"]))

    # Every interval references a known meter
    assert interval_keys.issubset(meter_keys)
