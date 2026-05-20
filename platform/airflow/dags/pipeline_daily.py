"""
pipeline_daily.py
=================
Airflow DAG: lakehouse_daily
Schedule  : 0 2 * * *  (runs at 02:00 every day)
Purpose   : Orchestrates the full Medallion pipeline:
            Bronze (stg) → Silver → Gold dims/facts → MySQL cache → dbt test

Failure handling
----------------
Every task writes its failure metadata to /tmp/pipeline_failures.log via
on_failure_callback so ops can correlate failures without opening the Airflow UI.
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
DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET = "prod"
FAILURE_LOG = "/tmp/pipeline_failures.log"

# Trino endpoint — override via TRINO_HOST / TRINO_PORT env vars so this DAG
# works across local / staging / production without code changes.
# Both values are shell-quoted at build time so they are safe to embed in any
# BashOperator command string, even if the env var contains special characters.
import shlex as _shlex
_TRINO_HOST_RAW = os.environ.get("TRINO_HOST", "trino")  # Docker service name in prod
_TRINO_PORT_RAW = os.environ.get("TRINO_PORT", "8080")
TRINO_SERVER = _shlex.quote(f"{_TRINO_HOST_RAW}:{_TRINO_PORT_RAW}")

# Allowed selector strings are validated at DAG-parse time against this set.
# Adding a new model selector requires an explicit entry here, which prevents
# arbitrary shell injection through the select argument.
_ALLOWED_SELECTORS: frozenset[str] = frozenset(
    [
        "stg_bronze_tickets",
        "stg_silver_tickets",
        "gold.dims.*",
        "fact_ticket_day_wide",
        "fact_ticket_hour_wide",
        "cache_ticket_daily",
    ]
)


def _dbt_run(select: str) -> str:
    """Return a safe dbt run bash command for *select*.

    Only selectors pre-registered in _ALLOWED_SELECTORS are permitted.
    This prevents shell injection if the selector value were ever sourced
    from user-controlled data (e.g. DAG params or XCom).
    """
    if select not in _ALLOWED_SELECTORS:
        raise ValueError(
            f"dbt selector {select!r} is not in the allowed list. "
            f"Register it in _ALLOWED_SELECTORS first."
        )
    # shlex.quote is used as a defence-in-depth second layer even though the
    # allowlist above is the primary control.
    import shlex
    safe_select = shlex.quote(select)
    return (
        f"cd {shlex.quote(DBT_DIR)} && "
        f"PATH=/pip-packages/bin:$PATH "
        f"dbt run --select {safe_select} "
        f"--profiles-dir {shlex.quote(DBT_PROFILES_DIR)} "
        f"--target {shlex.quote(DBT_TARGET)}"
    )


def _dbt_test() -> str:
    import shlex
    return (
        f"cd {shlex.quote(DBT_DIR)} && "
        f"dbt test "
        f"--profiles-dir {shlex.quote(DBT_PROFILES_DIR)} "
        f"--target {shlex.quote(DBT_TARGET)}"
    )


# ── Callbacks ─────────────────────────────────────────────────────────────────
def on_failure_callback(context: dict) -> None:
    """Append a JSON failure record to FAILURE_LOG for ops alerting.

    All context lookups use .get() with safe defaults so the callback never
    raises AttributeError / KeyError and masks the original failure.
    """
    dag_obj = context.get("dag")
    ti_obj = context.get("task_instance")
    record = {
        "dag_id": dag_obj.dag_id if dag_obj is not None else "unknown",
        "task_id": ti_obj.task_id if ti_obj is not None else "unknown",
        "execution_date": str(context.get("execution_date", "")),
        "log_url": ti_obj.log_url if ti_obj is not None else "",
        "exception": str(context.get("exception", "")),
        "timestamp": datetime.utcnow().isoformat(),
    }
    try:
        os.makedirs(os.path.dirname(os.path.abspath(FAILURE_LOG)), exist_ok=True)
        with open(FAILURE_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        log.error("Pipeline failure recorded: %s", record)
    except OSError as exc:
        log.error("Could not write failure log: %s", exc)


# ── Completion notifier ────────────────────────────────────────────────────────
def notify_complete(**context) -> None:
    """Log pipeline completion stats to stdout (visible in Airflow task logs).

    All context keys are accessed via .get() with safe defaults so this
    callable never raises KeyError if Airflow injects an unexpected context
    shape (e.g. during unit tests or manual backfills).
    """
    dag_run = context.get("dag_run")
    ti = context.get("task_instance")
    execution_date = context.get("execution_date", "")

    durations: dict[str, float] = {}
    if dag_run is not None and ti is not None:
        for task in dag_run.get_task_instances():
            if task.task_id != ti.task_id and task.duration is not None:
                durations[task.task_id] = round(task.duration, 1)

    summary = {
        "dag_id": dag_run.dag_id if dag_run is not None else "unknown",
        "run_id": dag_run.run_id if dag_run is not None else "unknown",
        "execution_date": str(execution_date),
        "status": "success",
        "total_wall_seconds": sum(durations.values()),
        "task_durations_sec": durations,
    }
    log.info("=== Pipeline complete ===\n%s", json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


# ── DAG definition ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="lakehouse_daily",
    description="Daily medallion pipeline: Bronze→Silver→Gold→Cache",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "medallion", "daily"],
    max_active_runs=1,
    doc_md="""
### lakehouse_daily

Runs every night at 02:00 UTC.  Covers:

1. **check_source_data** — asserts that new raw_tickets arrived in the last 24 h.
2. **bronze_silver** task group — incremental dbt bronze staging then silver merge.
3. **gold** task group — dimension tables first, then the wide fact table.
4. **dbt_cache** — writes daily aggregates to MySQL cache.
5. **dbt_test** — full dbt test suite gate.
6. **notify_complete** — logs wall-clock summary.

Failures are logged to `/tmp/pipeline_failures.log`.
""",
) as dag:

    # ── 1. Check source data freshness ────────────────────────────────────────
    # TRINO_SERVER is resolved at DAG-parse time from the environment variable
    # so the same DAG file works in local (localhost:8080) and prod (trino:8080).
    check_source_data = BashOperator(
        task_id="check_source_data",
        bash_command=(
            f"trino --server {TRINO_SERVER} --catalog iceberg --schema bronze "
            "--execute \"SELECT count(*) AS new_rows "
            "FROM iceberg.bronze.raw_tickets "
            "WHERE ingested_at >= current_timestamp - INTERVAL '1' DAY\" "
            "| tail -n 1 "
            "| awk '{if ($1+0 == 0) {print \"ERROR: No new rows in raw_tickets\"; exit 1} "
            "else print \"OK: \" $1 \" new rows\"}'"
        ),
        doc_md=(
            "Assert that raw_tickets has had at least one row ingested in the past 24 hours. "
            f"Connects to Trino at {TRINO_SERVER} (override via TRINO_HOST / TRINO_PORT env vars)."
        ),
    )

    # ── 2. Bronze → Silver task group ─────────────────────────────────────────
    with TaskGroup(group_id="bronze_silver") as bronze_silver_group:

        dbt_bronze = BashOperator(
            task_id="dbt_bronze",
            bash_command=_dbt_run("stg_bronze_tickets"),
            doc_md="Incremental append from raw Iceberg layer into bronze staging.",
        )

        dbt_silver = BashOperator(
            task_id="dbt_silver",
            bash_command=_dbt_run("stg_silver_tickets"),
            doc_md="Incremental merge (dedup + conform) from bronze into silver.",
        )

        dbt_bronze >> dbt_silver

    # ── 3. Gold task group ────────────────────────────────────────────────────
    with TaskGroup(group_id="gold") as gold_group:

        dbt_gold_dims = BashOperator(
            task_id="dbt_gold_dims",
            bash_command=_dbt_run("gold.dims.*"),
            doc_md="Rebuild all gold dimension tables (full table materialisation).",
        )

        dbt_gold_facts = BashOperator(
            task_id="dbt_gold_facts",
            bash_command=_dbt_run("fact_ticket_day_wide"),
            doc_md="Incremental merge into the wide fact table partitioned by date.",
        )

        dbt_gold_dims >> dbt_gold_facts

    # ── 4. MySQL cache ────────────────────────────────────────────────────────
    dbt_cache = BashOperator(
        task_id="dbt_cache",
        bash_command=_dbt_run("cache_ticket_daily"),
        doc_md=(
            "Write daily aggregate metrics into MySQL cache_ticket_daily "
            "for low-latency BI consumption by Metabase."
        ),
    )

    # ── 5. dbt test gate ──────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt_test(),
        doc_md="Run the full dbt test suite.  Failures here block the notify step.",
    )

    # ── 6. Completion notifier ────────────────────────────────────────────────
    notify = PythonOperator(
        task_id="notify_complete",
        python_callable=notify_complete,
        doc_md="Log a JSON summary of the pipeline run to stdout.",
    )

    # ── Task ordering ─────────────────────────────────────────────────────────
    check_source_data >> bronze_silver_group >> gold_group >> dbt_cache >> dbt_test >> notify
