#!/usr/bin/env python3
"""5. Aggregate — request count by HTTP method (GET, POST, …)."""

from pyspark.sql import functions as F

from httpd_logs import run_demo


def main(logs):
    (
        logs.groupBy("method")
        .agg(F.count("*").alias("requests"))
        .orderBy(F.col("requests").desc())
        .show(truncate=False)
    )


if __name__ == "__main__":
    run_demo("HttpdDemo05ByMethod", main)
