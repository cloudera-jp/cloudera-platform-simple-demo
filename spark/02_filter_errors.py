#!/usr/bin/env python3
"""2. Filter — HTTP 4xx / 5xx responses."""

from pyspark.sql import functions as F

from httpd_logs import run_demo


def main(logs):
    (
        logs.filter(F.col("status") >= 400)
        .select("client_ip", "method", "path", "status", "authuser", "log_date")
        .orderBy(F.col("status").desc())
        .show(20, truncate=60)
    )


if __name__ == "__main__":
    run_demo("HttpdDemo02FilterErrors", main)
