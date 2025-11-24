# gridflow/pipelines/build_product.py

from gridflow.io_landing import load_all_raw
from gridflow.io_standardized import (
    build_standardized_service_points,
    standardize_meters,
    standardize_intervals,
)
from gridflow.io_product import build_customer_usage_interval


def main():
    raw_all = load_all_raw()
    sp = build_standardized_service_points(raw_all)
    mt = standardize_meters(raw_all)
    iv = standardize_intervals(raw_all)

    usage = build_customer_usage_interval(sp, mt, iv)

    print("Customer usage (Product layer) shape:", usage.shape)
    print(usage.head())


if __name__ == "__main__":
    main()
