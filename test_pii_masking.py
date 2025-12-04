# test_pii_masking.py

from gridflow.io_landing import load_all_raw
from gridflow.io_standardized import (
    build_standardized_service_points,
    standardize_meters,
    standardize_intervals,
)
from gridflow.io_product import build_customer_usage_interval

raw = load_all_raw()
sp = build_standardized_service_points(raw)
mt = standardize_meters(raw)
iv = standardize_intervals(raw)

# Test external (masked)
usage_external = build_customer_usage_interval(sp, mt, iv, pii_access_level="external")

# Test internal (unmasked)
usage_internal = build_customer_usage_interval(sp, mt, iv, pii_access_level="internal")

print("=== EXTERNAL (masked) ===")
print(usage_external[["service_point_id", "street", "city", "zip"]].head(10))

print("\n=== INTERNAL (unmasked) ===")
print(usage_internal[["service_point_id", "street", "city", "zip"]].head(10))