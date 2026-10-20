#!/usr/bin/env python3
"""3. Filter — admin account, curl client, or 403 Forbidden."""

from pyspark.sql import functions as F

from httpd_logs import run_demo


def main(logs):
    (
        logs.filter(
            (F.col("authuser") == "admin")
            | F.col("line").contains("curl/")
            | (F.col("status") == 403)
        )
        .select("client_ip", "authuser", "method", "path", "status", "log_date")
        .orderBy(F.col("status").desc())
        .show(20, truncate=50)
    )


if __name__ == "__main__":
    run_demo("HttpdDemo03FilterSuspicious", main)
