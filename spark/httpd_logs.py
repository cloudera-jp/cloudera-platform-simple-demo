"""Shared helpers for httpd log PySpark demos (same directory as 01_*.py … 07_*.py)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Apache combined log: ip ident user [date] "METHOD path HTTP/x.x" status bytes ...
COMBINED_LOG = (
    r"^(\S+)\s+\S+\s+(\S+)\s+\[([^\]]+)\]\s+"
    r'"(\S+)\s+(\S+)\s+(\S+)"\s+(\d+)\s+(\d+)'
)

HDFS_GLOB = "/data/logs/httpd/access.log_*"


def default_input_path(local: bool) -> str:
    if not local:
        return HDFS_GLOB
    log_dir = Path(__file__).resolve().parent.parent / "data" / "logs" / "httpd"
    if not log_dir.is_dir():
        print(f"Local log directory not found: {log_dir}", file=sys.stderr)
        sys.exit(1)
    return f"file://{log_dir.resolve()}/access.log_*"


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=None, help=f"Default: {HDFS_GLOB}")
    parser.add_argument("--local", action="store_true", help="Use repo data/logs/httpd")
    return parser.parse_args()


def create_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_httpd_logs(spark: SparkSession, input_glob: str) -> DataFrame:
    raw = spark.read.text(input_glob)
    return (
        raw.withColumn("line", F.col("value"))
        .withColumn("client_ip", F.regexp_extract("value", COMBINED_LOG, 1))
        .withColumn("authuser", F.regexp_extract("value", COMBINED_LOG, 2))
        .withColumn("log_date", F.regexp_extract("value", COMBINED_LOG, 3))
        .withColumn("method", F.regexp_extract("value", COMBINED_LOG, 4))
        .withColumn("path", F.regexp_extract("value", COMBINED_LOG, 5))
        .withColumn("status", F.regexp_extract("value", COMBINED_LOG, 7).cast("int"))
        .withColumn("input_file", F.input_file_name())
        .filter(F.col("client_ip") != "")
    )


def run_demo(app_name: str, demo_fn: Callable[[DataFrame], None]) -> None:
    args = parse_cli_args()
    input_glob = args.input or default_input_path(args.local)
    spark = create_spark(app_name)
    print(f"Reading: {input_glob}")
    logs = load_httpd_logs(spark, input_glob)
    demo_fn(logs)
    spark.stop()
