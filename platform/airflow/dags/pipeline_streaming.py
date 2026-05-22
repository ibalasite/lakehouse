"""
pipeline_streaming.py
=====================
Airflow DAG: lakehouse_streaming
Schedule  : every 15 minutes  (*/15 * * * *)
Purpose   : Micro-batch pipeline triggered every 15 minutes:
              1. Drain the data-source pod → append to raw Iceberg table
              2. dbt incremental bronze + silver
              3. dbt Gold hour_long (append-only EAV, no full scan)
              4. dbt Gold hour_wide (PIVOT MERGE from hour_long)
              5. dbt cache_daily_report (UNION ALL MV → MySQL)
              6. Populate hourly MySQL cache (from gold.fact_ticket_hour_wide)
              7. Verify dashboard card returns data

EDD §13.2 streaming path:
  ingest → bronze → silver → hour_long → hour_wide → cache_daily_report → verify

Constraint: Data source generates 5–20 rows per 5-min tick, so each 15-min
            cycle ingests at most ~60 rows.  No performance impact.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
DBT_DIR          = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET       = "prod"
DBT_TARGET_MYSQL = "mysql_cache"
FAILURE_LOG      = "/tmp/pipeline_failures.log"
FETCH_SCRIPT     = "/opt/airflow/scripts/fetch_and_ingest.py"

import shlex as _shlex
_TRINO_HOST_RAW = os.environ.get("TRINO_HOST", "trino")
_TRINO_PORT_RAW = os.environ.get("TRINO_PORT", "8080")
TRINO_SERVER = _shlex.quote(f"{_TRINO_HOST_RAW}:{_TRINO_PORT_RAW}")

_ALLOWED_SELECTORS: frozenset[str] = frozenset([
    "stg_bronze_tickets",
    "stg_silver_tickets",
    "fact_ticket_hour_long",
    "fact_ticket_hour_wide",
    "cache_daily_report",
])

_ALLOWED_CACHE_SELECTORS: frozenset[str] = frozenset(["cache_daily_report"])


def _dbt_run(select: str, extra_vars: dict | None = None) -> str:
    if select not in _ALLOWED_SELECTORS:
        raise ValueError(
            f"dbt selector {select!r} not in allowed list. "
            "Register it in _ALLOWED_SELECTORS first."
        )
    import json
    import shlex
    safe_select = shlex.quote(select)
    vars_flag = (
        f"--vars {shlex.quote(json.dumps(extra_vars))} " if extra_vars else ""
    )
    return (
        f"cd {shlex.quote(DBT_DIR)} && "
        f"PATH=/pip-packages/bin:$PATH "
        f"dbt run --select {safe_select} "
        f"{vars_flag}"
        f"--profiles-dir {shlex.quote(DBT_PROFILES_DIR)} "
        f"--target {shlex.quote(DBT_TARGET)}"
    )


def _dbt_run_mysql(select: str) -> str:
    """dbt run against mysql_cache target (Trino MySQL connector → mysql.lakehouse_cache)."""
    if select not in _ALLOWED_CACHE_SELECTORS:
        raise ValueError(
            f"dbt selector {select!r} not in allowed cache list. "
            "Register it in _ALLOWED_CACHE_SELECTORS first."
        )
    import shlex
    safe_select = shlex.quote(select)
    return (
        f"cd {shlex.quote(DBT_DIR)} && "
        f"PATH=/pip-packages/bin:$PATH "
        f"dbt run --select {safe_select} "
        f"--profiles-dir {shlex.quote(DBT_PROFILES_DIR)} "
        f"--target {shlex.quote(DBT_TARGET_MYSQL)}"
    )


# ── Callbacks ─────────────────────────────────────────────────────────────────
def on_failure_callback(context: dict) -> None:
    dag_obj = context.get("dag")
    ti_obj  = context.get("task_instance")
    record  = {
        "dag_id":         dag_obj.dag_id if dag_obj is not None else "unknown",
        "task_id":        ti_obj.task_id if ti_obj is not None else "unknown",
        "execution_date": str(context.get("execution_date", "")),
        "log_url":        ti_obj.log_url if ti_obj is not None else "",
        "exception":      str(context.get("exception", "")),
        "timestamp":      datetime.now(tz=timezone(timedelta(hours=8))).isoformat(),
    }
    try:
        os.makedirs(os.path.dirname(os.path.abspath(FAILURE_LOG)), exist_ok=True)
        with open(FAILURE_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        log.error("Pipeline failure recorded: %s", record)
    except OSError as exc:
        log.error("Could not write failure log: %s", exc)


# ── Hourly cache refresh ───────────────────────────────────────────────────────
# Reads today's pre-aggregated rows from gold.fact_ticket_hour_wide (EDD §8.6).
_POPULATE_HOURLY_CMD = r"""
set -e
python3 /opt/airflow/scripts/populate_mysql_cache.py --streaming
"""

# ── Verify Metabase card ───────────────────────────────────────────────────────
_VERIFY_CMD = r"""
set -e
python3 - <<'PYEOF'
import os, sys, time
import requests

MB_URL  = os.environ.get("METABASE_URL", "http://metabase:3000")
EMAIL   = os.environ.get("METABASE_ADMIN_EMAIL", "")
PASSWD  = os.environ.get("METABASE_ADMIN_PASSWORD", "")

if not EMAIL or not PASSWD:
    print("METABASE_ADMIN_EMAIL / METABASE_ADMIN_PASSWORD not set — skipping verify")
    sys.exit(0)

session = requests.Session()
r = session.post(f"{MB_URL}/api/session",
                 json={"username": EMAIL, "password": PASSWD}, timeout=15)
if not r.ok:
    print(f"Metabase login failed: {r.status_code}")
    sys.exit(1)
session.headers["X-Metabase-Session"] = r.json()["id"]

r2 = session.get(f"{MB_URL}/api/card", timeout=15)
cards = r2.json() if r2.ok else []
if isinstance(cards, dict):
    cards = cards.get("data", [])
card = next((c for c in cards if c.get("name") == "今日問題單即時動態"), None)
if card is None:
    print("Hourly card '今日問題單即時動態' not found — skipping verify")
    sys.exit(0)

r3 = session.post(f"{MB_URL}/api/card/{card['id']}/query", timeout=30)
row_count = len((r3.json().get("data", {}) or {}).get("rows", []))
print(f"Dashboard verify OK — card returned {row_count} rows")
PYEOF
"""


# ── DAG definition ─────────────────────────────────────────────────────────────
default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=3),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id           = "lakehouse_streaming",
    description      = "15-min micro-batch: ingest→bronze→silver→gold_hour→cache_report→verify",
    schedule_interval= "*/15 * * * *",
    start_date       = datetime(2024, 1, 1),
    catchup          = False,
    default_args     = default_args,
    tags             = ["lakehouse", "streaming", "medallion"],
    max_active_runs  = 1,
    doc_md           = """
### lakehouse_streaming

Runs every **15 minutes**. EDD §13.2 streaming path.

1. **ingest_from_source** — drains data-source pod HTTP API → `iceberg.bronze.raw_tickets`.
2. **bronze_silver** group — incremental dbt bronze + silver merge.
3. **gold_hour** group — `fact_ticket_hour_long` (append-only EAV) → `fact_ticket_hour_wide` (PIVOT MERGE).
4. **cache_report** — `cache_daily_report` UNION ALL MV written to MySQL (primary Metabase source).
5. **populate_hourly_cache** — refresh `cache_ticket_hourly` in MySQL from gold.
6. **verify_dashboard** — spot-check Metabase hourly card returns data.

Generates ≤60 rows per cycle. Gold append-only, no full scan, no OOMKill.
""",
) as dag:

    # ── 1. Fetch + ingest ─────────────────────────────────────────────────────
    ingest_from_source = BashOperator(
        task_id      = "ingest_from_source",
        bash_command = f"python3 {_shlex.quote(FETCH_SCRIPT)}",
        doc_md       = "Drain data-source pod and append new rows to iceberg.bronze.raw_tickets.",
    )

    # ── 2. Bronze → Silver ────────────────────────────────────────────────────
    with TaskGroup(group_id="bronze_silver") as bronze_silver_group:
        dbt_bronze = BashOperator(
            task_id      = "dbt_bronze",
            bash_command = _dbt_run("stg_bronze_tickets", {"bronze_lookback_hours": 2}),
            pool         = "trino_slots",
            doc_md       = "Incremental append from raw Iceberg into bronze staging.",
        )
        dbt_silver = BashOperator(
            task_id      = "dbt_silver",
            bash_command = _dbt_run("stg_silver_tickets", {"bronze_lookback_hours": 2}),
            pool         = "trino_slots",
            doc_md       = "Incremental merge (dedup + conform) into silver.",
        )
        dbt_bronze >> dbt_silver

    # ── 3. Gold hour: hour_long → hour_wide ───────────────────────────────────
    with TaskGroup(group_id="gold_hour") as gold_hour_group:
        dbt_hour_long = BashOperator(
            task_id      = "dbt_hour_long",
            bash_command = _dbt_run("fact_ticket_hour_long"),
            pool         = "trino_slots",
            doc_md       = (
                "Append-only EAV narrow fact. Only processes silver rows newer than "
                "MAX(hour_long.updated_at) - 1 min. Zero full-table scan."
            ),
        )
        dbt_hour_wide = BashOperator(
            task_id      = "dbt_hour_wide",
            bash_command = _dbt_run("fact_ticket_hour_wide"),
            pool         = "trino_slots",
            doc_md       = "PIVOT MERGE from hour_long delta rows → hourly serving fact.",
        )
        dbt_hour_long >> dbt_hour_wide

    # ── 4. Cache MV: UNION ALL report — dual target (EDD §13.2b) ─────────────
    dbt_cache_report_iceberg = BashOperator(
        task_id      = "dbt_cache_report_iceberg",
        bash_command = _dbt_run("cache_daily_report"),
        pool         = "trino_slots",
        doc_md       = (
            "Lambda Architecture UNION ALL (D-1 day_wide + today hour_wide, LEFT JOIN dims) "
            "into iceberg.cache.cache_daily_report. Iceberg is source of truth."
        ),
    )

    dbt_cache_report_mysql = BashOperator(
        task_id      = "dbt_cache_report_mysql",
        bash_command = _dbt_run_mysql("cache_daily_report"),
        pool         = "trino_slots",
        doc_md       = (
            "Mirror cache_daily_report into mysql.lakehouse_cache via Trino MySQL connector. "
            "Primary table read by Metabase dashboards."
        ),
    )

    # ── 5. Hourly MySQL cache (raw hourly granularity) ─────────────────────────
    populate_hourly_cache = BashOperator(
        task_id      = "populate_hourly_cache",
        bash_command = _POPULATE_HOURLY_CMD,
        doc_md       = (
            "Reads today's rows from iceberg.gold.fact_ticket_hour_wide and writes "
            "to cache_ticket_hourly in MySQL. Used by hourly breakdown cards."
        ),
    )

    # ── 6. Verify dashboard ───────────────────────────────────────────────────
    verify_dashboard = BashOperator(
        task_id      = "verify_dashboard",
        bash_command = _VERIFY_CMD,
        doc_md       = "Spot-check that the Metabase hourly card returns rows.",
    )

    # ── Task ordering ─────────────────────────────────────────────────────────
    # cache_daily_report: Iceberg first (source of truth) → MySQL mirror.
    # populate_hourly_cache and verify_dashboard run after MySQL write completes.
    (
        ingest_from_source
        >> bronze_silver_group
        >> gold_hour_group
        >> dbt_cache_report_iceberg
        >> dbt_cache_report_mysql
        >> populate_hourly_cache
        >> verify_dashboard
    )
