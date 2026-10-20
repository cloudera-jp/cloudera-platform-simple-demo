#!/usr/bin/env python3
"""1. Overview — how many log lines and files are on HDFS."""

from httpd_logs import run_demo


def main(logs):
    total = logs.count()
    files = logs.select("input_file").distinct().count()
    print(f"Parsed log lines: {total}")
    print(f"Source files:     {files}")

    print("\nFirst 10 log lines:")
    (
        logs.select("client_ip", "authuser", "method", "path", "status", "log_date")
        .limit(10)
        .show(truncate=40)
    )


if __name__ == "__main__":
    run_demo("HttpdDemo01Overview", main)
