#!/usr/bin/env python3
"""6. Aggregate — top 10 client IPs (with error request count)."""

from pyspark.sql import functions as F

from httpd_logs import run_demo


def main(logs):
    (
        logs.groupBy("client_ip")
        .agg(
            F.count("*").alias("requests"),
            F.sum(F.when(F.col("status") >= 400, 1).otherwise(0)).alias("error_requests"),
        )
        .orderBy(F.col("requests").desc())
        .limit(10)
        .show(truncate=False)
    )


if __name__ == "__main__":
    run_demo("HttpdDemo06TopIps", main)
