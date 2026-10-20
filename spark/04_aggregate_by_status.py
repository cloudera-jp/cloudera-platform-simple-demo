#!/usr/bin/env python3
"""4. Aggregate — request count by HTTP status code."""

from pyspark.sql import functions as F

from httpd_logs import run_demo


def main(logs):
    (
        logs.groupBy("status")
        .agg(F.count("*").alias("requests"))
        .orderBy(F.col("requests").desc())
        .show(truncate=False)
    )


if __name__ == "__main__":
    run_demo("HttpdDemo04ByStatus", main)
