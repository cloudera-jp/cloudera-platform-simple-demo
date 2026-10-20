"""FastAPI: aggregate demo_logs.weblog (HDFS /data/logs/httpd) via Spark SQL."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query
from pyspark.sql import SparkSession

DEFAULT_APP_ROOT = "/home/cdsw/demo_source/fastapi"
APP_ROOT = Path(os.environ.get("FASTAPI_APP_ROOT", DEFAULT_APP_ROOT))
if not APP_ROOT.is_dir():
    APP_ROOT = Path(__file__).resolve().parent

TABLE = os.environ.get("WEBLOG_TABLE", "demo_logs.weblog")

app = FastAPI(
    title="Weblog Stats API",
    description="Simple aggregations over httpd access logs on the data lake.",
    version="1.0.0",
)

spark = (
    SparkSession.builder.appName("WeblogFastAPI")
    .config("spark.executor.instances", "2")
    .config("spark.dynamicAllocation.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def sql_rows(query: str) -> list[dict[str, Any]]:
    df = spark.sql(query)
    return [row.asDict(recursive=True) for row in df.collect()]


@app.get("/")
def root() -> dict[str, str]:
    return {
        "service": "weblog-stats-api",
        "table": TABLE,
        "docs": "/docs",
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/v1/summary")
def summary() -> dict[str, Any]:
    rows = sql_rows(
        f"""
        SELECT
          COUNT(*) AS total_requests,
          COUNT(DISTINCT client_ip) AS distinct_client_ips,
          SUM(CASE WHEN CAST(status AS INT) >= 400 THEN 1 ELSE 0 END) AS error_requests
        FROM {TABLE}
        """
    )
    return {"table": TABLE, "summary": rows[0] if rows else {}}


@app.get("/v1/stats/by-status")
def stats_by_status(
    min_status: int | None = Query(None, ge=100, le=599, description="例: 400 で 4xx/5xx のみ"),
) -> dict[str, Any]:
    where = ""
    if min_status is not None:
        where = f"WHERE CAST(status AS INT) >= {int(min_status)}"
    rows = sql_rows(
        f"""
        SELECT CAST(status AS INT) AS status, COUNT(*) AS requests
        FROM {TABLE}
        {where}
        GROUP BY CAST(status AS INT)
        ORDER BY requests DESC
        """
    )
    return {"table": TABLE, "min_status": min_status, "items": rows}


@app.get("/v1/stats/by-method")
def stats_by_method() -> dict[str, Any]:
    rows = sql_rows(
        f"""
        SELECT method, COUNT(*) AS requests
        FROM {TABLE}
        GROUP BY method
        ORDER BY requests DESC
        """
    )
    return {"table": TABLE, "items": rows}


@app.get("/v1/stats/top-client-ips")
def stats_top_client_ips(
    limit: int = Query(10, ge=1, le=100),
    errors_only: bool = Query(False, description="true のとき 4xx/5xx のみ集計"),
) -> dict[str, Any]:
    where = "WHERE CAST(status AS INT) >= 400" if errors_only else ""
    rows = sql_rows(
        f"""
        SELECT
          client_ip,
          COUNT(*) AS requests,
          SUM(CASE WHEN CAST(status AS INT) >= 400 THEN 1 ELSE 0 END) AS error_requests
        FROM {TABLE}
        {where}
        GROUP BY client_ip
        ORDER BY requests DESC
        LIMIT {int(limit)}
        """
    )
    return {"table": TABLE, "limit": limit, "errors_only": errors_only, "items": rows}


def _run_uvicorn(host: str, port: int) -> None:
    import uvicorn

    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    import threading

    port = int(os.environ["CDSW_APP_PORT"])
    host = "127.0.0.1"
    server_thread = threading.Thread(
        target=_run_uvicorn,
        args=(host, port),
        name="uvicorn",
        daemon=False,
    )
    try:
        server_thread.start()
        print(f"Weblog Stats API listening on http://{host}:{port}")
        server_thread.join()
    except Exception as exc:
        print(f"ERROR: unable to run application:\n {exc}")
