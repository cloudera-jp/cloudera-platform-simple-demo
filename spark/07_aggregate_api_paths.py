#!/usr/bin/env python3
"""7. Aggregate — /api* and login/logout paths by status."""

from pyspark.sql import functions as F

from httpd_logs import run_demo


def main(logs):
    api = logs.filter(
        F.col("path").startswith("/api") | F.col("path").isin("/login", "/logout")
    )
    (
        api.groupBy("path", "status")
        .agg(F.count("*").alias("requests"))
        .orderBy(F.col("requests").desc())
        .limit(15)
        .show(truncate=False)
    )


if __name__ == "__main__":
    run_demo("HttpdDemo07ApiPaths", main)
