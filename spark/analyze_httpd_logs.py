#!/usr/bin/env python3
"""
Run all httpd log demos (01–07) in one shot.

For step-by-step presentation, use the numbered scripts instead:
  spark-submit 01_overview.py
  spark-submit 02_filter_errors.py
  …
"""

from httpd_logs import create_spark, default_input_path, load_httpd_logs, parse_cli_args

import importlib


DEMOS = (
    "01_overview",
    "02_filter_errors",
    "03_filter_suspicious",
    "04_aggregate_by_status",
    "05_aggregate_by_method",
    "06_aggregate_top_ips",
    "07_aggregate_api_paths",
)


def main() -> None:
    args = parse_cli_args()
    input_glob = args.input or default_input_path(args.local)
    spark = create_spark("HttpdAccessLogDemoAll")
    print(f"Reading: {input_glob}")
    logs = load_httpd_logs(spark, input_glob)

    for name in DEMOS:
        mod = importlib.import_module(name)
        print(f"\n=== {name} ===")
        mod.main(logs)

    spark.stop()


if __name__ == "__main__":
    main()
