"""Flask Workbench app: browse demo_logs.weblog via Spark SQL."""

from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, render_template, request
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DEFAULT_APP_ROOT = "/home/cdsw/demo_source/flask"
APP_ROOT = Path(os.environ.get("FLASK_APP_ROOT", DEFAULT_APP_ROOT))
if not (APP_ROOT / "templates").is_dir():
    APP_ROOT = Path(__file__).resolve().parent

TABLE = "demo_logs.weblog"
MAX_ROWS = 500

DROPDOWN_COLUMNS = ("method", "path", "http_version", "status")
TEXT_COLUMNS = (
    "client_ip",
    "ident",
    "authuser",
    "log_datetime",
    "bytes",
    "referer",
    "user_agent",
)
DISPLAY_COLUMNS = DROPDOWN_COLUMNS + TEXT_COLUMNS

app = Flask(__name__, template_folder=str(APP_ROOT / "templates"))

# Demo: one Spark session per Application process (executors reused across requests).
spark = (
    SparkSession.builder.appName("WeblogFlaskApp")
    .config("spark.executor.instances", "2")
    .config("spark.dynamicAllocation.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def spark_rows_to_dicts(spark_df, limit: int | None = None) -> list[dict]:
    pdf = spark_df.limit(limit).toPandas() if limit else spark_df.toPandas()
    if pdf.empty:
        return []
    return pdf.where(pdf.notna(), None).to_dict("records")


def fetch_distinct_values(column: str) -> list[str]:
    query = f"""
        SELECT DISTINCT `{column}` AS value
        FROM {TABLE}
        WHERE `{column}` IS NOT NULL AND TRIM(`{column}`) != ''
        ORDER BY value
        LIMIT 500
    """
    rows = spark_rows_to_dicts(spark.sql(query))
    return [str(row["value"]) for row in rows]


def apply_filters(form: dict) -> list[dict]:
    df = spark.table(TABLE)
    for col in DROPDOWN_COLUMNS:
        value = (form.get(col) or "").strip()
        if value:
            df = df.filter(F.col(col) == value)
    for col in TEXT_COLUMNS:
        value = (form.get(col) or "").strip()
        if value:
            df = df.filter(F.col(col).contains(value))
    return spark_rows_to_dicts(df.select(*DISPLAY_COLUMNS), limit=MAX_ROWS)


@app.route("/", methods=["GET", "POST"])
def index():
    filter_options = {col: fetch_distinct_values(col) for col in DROPDOWN_COLUMNS}
    form_values = {c: "" for c in DISPLAY_COLUMNS}
    results: list[dict] = []

    if request.method == "POST":
        for col in DISPLAY_COLUMNS:
            form_values[col] = request.form.get(col, "").strip()
        results = apply_filters(form_values)
    else:
        cols = ", ".join(f"`{c}`" for c in DISPLAY_COLUMNS)
        results = spark_rows_to_dicts(
            spark.sql(f"SELECT {cols} FROM {TABLE}"),
            limit=MAX_ROWS,
        )

    return render_template(
        "index.html",
        results=results,
        form_values=form_values,
        filter_options=filter_options,
        display_columns=DISPLAY_COLUMNS,
        max_rows=MAX_ROWS,
        table_name=TABLE,
    )


if __name__ == "__main__":
    try:
        app.run(host="127.0.0.1", port=int(os.environ["CDSW_APP_PORT"]))
    except Exception as exc:
        print(f"ERROR: unable to run application:\n {exc}")
