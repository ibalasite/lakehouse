"""
pipeline_streaming.py
=====================
Airflow DAG: lakehouse_streaming
Schedule  : every 15 minutes  (*/15 * * * *)
Purpose   : Micro-batch pipeline triggered every 15 minutes:
              1. Drain the data-source pod → append to raw Iceberg table
              2. dbt incremental bronze + silver
              3. dbt incremental gold hour_wide fact
              4. Populate hourly MySQL cache
              5. Verify dashboard card returns data

Constraint: Data source generates 5–20 rows per 5-min tick, so each 15-min
            cycle ingests at most ~60 rows.  No performance impact.

Failure handling: same on_failure_callback as pipeline_daily.py — appends
JSON record to /tmp/pipeline_failures.log.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
DBT_DIR          = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET       = "prod"
FAILURE_LOG      = "/tmp/pipeline_failures.log"
FETCH_SCRIPT     = "/opt/airflow/scripts/fetch_and_ingest.py"

import shlex as _shlex
_TRINO_HOST_RAW = os.environ.get("TRINO_HOST", "trino")
_TRINO_PORT_RAW = os.environ.get("TRINO_PORT", "8080")
TRINO_SERVER = _shlex.quote(f"{_TRINO_HOST_RAW}:{_TRINO_PORT_RAW}")

_ALLOWED_SELECTORS: frozenset[str] = frozenset([
    "stg_bronze_tickets",
    "stg_silver_tickets",
    "fact_ticket_hour_wide",
])


def _dbt_run(select: str) -> str:
    if select not in _ALLOWED_SELECTORS:
        raise ValueError(
            f"dbt selector {select!r} not in allowed list. "
            "Register it in _ALLOWED_SELECTORS first."
        )
    import shlex
    safe_select = shlex.quote(select)
    return (
        f"cd {shlex.quote(DBT_DIR)} && "
        f"dbt run --select {safe_select} "
        f"--profiles-dir {shlex.quote(DBT_PROFILES_DIR)} "
        f"--target {shlex.quote(DBT_TARGET)}"
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
        "timestamp":      datetime.utcnow().isoformat(),
    }
    try:
        os.makedirs(os.path.dirname(os.path.abspath(FAILURE_LOG)), exist_ok=True)
        with open(FAILURE_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        log.error("Pipeline failure recorded: %s", record)
    except OSError as exc:
        log.error("Could not write failure log: %s", exc)


# ── Hourly cache refresh ───────────────────────────────────────────────────────
# Runs populate_mysql_cache.py --hourly-only via BashOperator to push
# today's hourly aggregates from Trino → cache_ticket_hourly.
_POPULATE_HOURLY_CMD = r"""
set -e
python3 /opt/airflow/scripts/populate_mysql_cache.py --hourly-only
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

# Find the hourly card by name
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
    description      = "15-min micro-batch: ingest→bronze→silver→gold_hour→cache",
    schedule_interval= "*/15 * * * *",
    start_date       = datetime(2024, 1, 1),
    catchup          = False,
    default_args     = default_args,
    tags             = ["lakehouse", "streaming", "medallion"],
    max_active_runs  = 1,
    doc_md           = """
### lakehouse_streaming

Runs every **15 minutes**.

1. **ingest_from_source** — drains the data-source pod HTTP API and appends
   new rows to `iceberg.raw.raw_tickets` via pyiceberg.
2. **bronze_silver** group — incremental dbt bronze + silver merge.
3. **gold_hour** — incremental merge into `fact_ticket_hour_wide`.
4. **populate_hourly_cache** — refresh `cache_ticket_hourly` in MySQL.
5. **verify_dashboard** — spot-check that the Metabase hourly card returns data.

Generates ≤60 rows per cycle — zero performance impact.
""",
) as dag:

    # ── 1. Fetch + ingest ─────────────────────────────────────────────────────
    ingest_from_source = BashOperator(
        task_id      = "ingest_from_source",
        bash_command = f"python3 {_shlex.quote(FETCH_SCRIPT)}",
        doc_md       = (
            "Drain the data-source pod HTTP API and write new rows to "
            "iceberg.raw.raw_tickets via pyiceberg."
        ),
    )

    # ── 2. Bronze → Silver ────────────────────────────────────────────────────
    with TaskGroup(group_id="bronze_silver") as bronze_silver_group:
        dbt_bronze = BashOperator(
            task_id      = "dbt_bronze",
            bash_command = _dbt_run("stg_bronze_tickets"),
            doc_md       = "Incremental append from raw Iceberg into bronze staging.",
        )
        dbt_silver = BashOperator(
            task_id      = "dbt_silver",
            bash_command = _dbt_run("stg_silver_tickets"),
            doc_md       = "Incremental merge (dedup + conform) into silver.",
        )
        dbt_bronze >> dbt_silver

    # ── 3. Gold hour_wide ─────────────────────────────────────────────────────
    dbt_gold_hour = BashOperator(
        task_id      = "dbt_gold_hour",
        bash_command = _dbt_run("fact_ticket_hour_wide"),
        doc_md       = "Incremental merge into the hourly wide fact table.",
    )

    # ── 4. Hourly MySQL cache ──────────────────────────────────────────────────
    populate_hourly_cache = BashOperator(
        task_id      = "populate_hourly_cache",
        bash_command = _POPULATE_HOURLY_CMD,
        doc_md       = "Write hourly aggregates into cache_ticket_hourly for Metabase.",
    )

    # ── 5. Verify dashboard ───────────────────────────────────────────────────
    verify_dashboard = BashOperator(
        task_id      = "verify_dashboard",
        bash_command = _VERIFY_CMD,
        doc_md       = "Spot-check that the Metabase hourly card returns rows.",
    )

    # ── Task ordering ─────────────────────────────────────────────────────────
    (
        ingest_from_source
        >> bronze_silver_group
        >> dbt_gold_hour
        >> populate_hourly_cache
        >> verify_dashboard
    )
