"""
pipeline_backfill.py
--------------------
DAG: lakehouse_backfill
Schedule: None (manual trigger only)

Full rebuild / historical backfill for the lakehouse medallion pipeline.
Run this when you need a complete rebuild of all layers — e.g. after schema
changes, after a large historical data correction, or on first deployment.

Parameters (configurable at trigger time via Airflow UI → Trigger w/ config):
    start_date  : ISO date, default "2022-01-01"
    end_date    : ISO date, default today's date

Usage from CLI:
    airflow dags trigger lakehouse_backfill \\
        --conf '{"start_date":"2022-01-01","end_date":"2026-05-01"}'
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ── Default args ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}

DBT_DIR = "/opt/airflow/dbt"
# PATH must include /pip-packages/bin so dbt and trino CLIs are found at runtime.
DBT_CMD = f"cd {DBT_DIR} && PATH=/pip-packages/bin:$PATH dbt"
DBT_FLAGS = "--profiles-dir /opt/airflow/dbt --target prod"
DBT_FLAGS_MYSQL = "--profiles-dir /opt/airflow/dbt --target mysql_cache"

# ── Callbacks ─────────────────────────────────────────────────────────────────

def _on_failure(context: dict) -> None:
    log.error(
        "[lakehouse_backfill] Task %s failed in DagRun %s",
        context["task_instance"].task_id,
        context["run_id"],
    )
    with open("/tmp/pipeline_failures.log", "a") as fh:
        fh.write(
            f"{datetime.utcnow().isoformat()}  backfill  "
            f"{context['task_instance'].task_id}  FAILED\n"
        )


def _log_params(**context: object) -> None:
    conf = context["dag_run"].conf or {}
    start = conf.get("start_date", "2022-01-01")
    end = conf.get("end_date", datetime.utcnow().strftime("%Y-%m-%d"))
    log.info("[lakehouse_backfill] start_date=%s  end_date=%s", start, end)


def _validate_row_counts(**context: object) -> None:
    """Smoke-check that key tables are non-empty after the rebuild."""
    import subprocess

    checks = {
        "silver": "SELECT count(*) FROM iceberg.silver.stg_silver_tickets",
        "gold_fact": "SELECT count(*) FROM iceberg.gold.fact_ticket_day_wide",
        "cache_daily": "SELECT count(*) FROM mysql.lakehouse_cache.cache_ticket_daily",
        "cache_hourly": "SELECT count(*) FROM mysql.lakehouse_cache.cache_ticket_hourly",
        "cache_report": "SELECT count(*) FROM mysql.lakehouse_cache.cache_daily_report",
    }
    # PATH must be set so the trino Python CLI at /pip-packages/bin/trino is found.
    trino = "PATH=/pip-packages/bin:$PATH trino --server http://trino:8080 --output-format TSV_HEADER"
    for name, sql in checks.items():
        result = subprocess.run(
            f'{trino} --execute "{sql}"',
            shell=True,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Row-count check failed for {name}: {result.stderr}")
        count = int(result.stdout.strip().splitlines()[-1].replace(",", ""))
        log.info("  %-20s  %s rows", name, f"{count:,}")
        if count == 0:
            raise ValueError(f"Table '{name}' has 0 rows after backfill!")
    log.info("[lakehouse_backfill] All row-count checks passed.")


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="lakehouse_backfill",
    description="Full-rebuild / backfill — manual trigger only",
    schedule_interval=None,           # never auto-run
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                # prevent concurrent full rebuilds
    default_args=DEFAULT_ARGS,
    on_failure_callback=_on_failure,
    tags=["lakehouse", "backfill"],
    params={
        "start_date": "2022-01-01",
        "end_date": datetime.utcnow().strftime("%Y-%m-%d"),
    },
) as dag:

    # ── 0. Log parameters ────────────────────────────────────────────────────
    log_params = PythonOperator(
        task_id="log_params",
        python_callable=_log_params,
    )

    # ── 1. dbt clean ─────────────────────────────────────────────────────────
    dbt_clean = BashOperator(
        task_id="dbt_clean",
        bash_command=f"{DBT_CMD} clean {DBT_FLAGS}",
    )

    # ── 2. dbt deps ──────────────────────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_CMD} deps {DBT_FLAGS}",
    )

    # ── 3. Bronze full-refresh ───────────────────────────────────────────────
    with TaskGroup(group_id="full_refresh_bronze") as tg_bronze:
        dbt_bronze_fr = BashOperator(
            task_id="dbt_bronze_full_refresh",
            bash_command=(
                f"{DBT_CMD} run {DBT_FLAGS} "
                "--select stg_bronze_tickets "
                "--full-refresh"
            ),
            execution_timeout=timedelta(hours=3),
        )

    # ── 4. Silver full-refresh ───────────────────────────────────────────────
    with TaskGroup(group_id="full_refresh_silver") as tg_silver:
        dbt_silver_fr = BashOperator(
            task_id="dbt_silver_full_refresh",
            bash_command=(
                f"{DBT_CMD} run {DBT_FLAGS} "
                "--select stg_silver_tickets "
                "--full-refresh"
            ),
            execution_timeout=timedelta(hours=2),
        )

    # ── 5. Gold — dims → hour_long → hour_wide → day_wide → month_wide ──────
    # EDD §13.2 order: dims first, then the full fact chain in sequence.
    with TaskGroup(group_id="gold_rebuild") as tg_gold:
        dbt_gold_dims = BashOperator(
            task_id="dbt_gold_dims",
            bash_command=(
                f"{DBT_CMD} run {DBT_FLAGS} "
                "--select gold.dims.* "
                "--full-refresh"
            ),
        )

        dbt_gold_hour_long = BashOperator(
            task_id="dbt_gold_hour_long",
            bash_command=(
                f"{DBT_CMD} run {DBT_FLAGS} "
                "--select fact_ticket_hour_long "
                "--full-refresh"
            ),
            execution_timeout=timedelta(hours=2),
        )

        dbt_gold_hour_wide = BashOperator(
            task_id="dbt_gold_hour_wide",
            bash_command=(
                f"{DBT_CMD} run {DBT_FLAGS} "
                "--select fact_ticket_hour_wide "
                "--full-refresh"
            ),
            execution_timeout=timedelta(hours=2),
        )

        dbt_gold_day_wide = BashOperator(
            task_id="dbt_gold_day_wide",
            bash_command=(
                f"{DBT_CMD} run {DBT_FLAGS} "
                "--select fact_ticket_day_wide "
                "--full-refresh"
            ),
            execution_timeout=timedelta(hours=2),
        )

        dbt_gold_month_wide = BashOperator(
            task_id="dbt_gold_month_wide",
            bash_command=(
                f"{DBT_CMD} run {DBT_FLAGS} "
                "--select fact_ticket_month_wide "
                "--full-refresh"
            ),
        )

        dbt_gold_dims >> dbt_gold_hour_long >> dbt_gold_hour_wide >> dbt_gold_day_wide >> dbt_gold_month_wide

    # ── 6. Cache rebuild — dual target (EDD §13.2b) ──────────────────────────
    # Each cache model runs twice: Iceberg (source of truth) then MySQL mirror.
    # Three independent chains run in parallel from gold_rebuild.

    dbt_cache_daily_iceberg = BashOperator(
        task_id="dbt_cache_daily_iceberg",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS} --select cache_ticket_daily",
        execution_timeout=timedelta(minutes=30),
        doc_md="Full rebuild of cache_ticket_daily into iceberg.cache.",
    )
    dbt_cache_daily_mysql = BashOperator(
        task_id="dbt_cache_daily_mysql",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS_MYSQL} --select cache_ticket_daily",
        execution_timeout=timedelta(minutes=30),
        doc_md="Mirror cache_ticket_daily into mysql.lakehouse_cache via Trino MySQL connector.",
    )

    dbt_cache_hourly_iceberg = BashOperator(
        task_id="dbt_cache_hourly_iceberg",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS} --select cache_ticket_hourly",
        execution_timeout=timedelta(minutes=30),
        doc_md="Full rebuild of cache_ticket_hourly into iceberg.cache.",
    )
    dbt_cache_hourly_mysql = BashOperator(
        task_id="dbt_cache_hourly_mysql",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS_MYSQL} --select cache_ticket_hourly",
        execution_timeout=timedelta(minutes=30),
        doc_md="Mirror cache_ticket_hourly into mysql.lakehouse_cache via Trino MySQL connector.",
    )

    dbt_cache_report_iceberg = BashOperator(
        task_id="dbt_cache_report_iceberg",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS} --select cache_daily_report",
        execution_timeout=timedelta(minutes=30),
        doc_md="Full rebuild of cache_daily_report (Lambda UNION ALL) into iceberg.cache.",
    )
    dbt_cache_report_mysql = BashOperator(
        task_id="dbt_cache_report_mysql",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS_MYSQL} --select cache_daily_report",
        execution_timeout=timedelta(minutes=30),
        doc_md="Mirror cache_daily_report into mysql.lakehouse_cache via Trino MySQL connector.",
    )

    # ── 7. dbt tests — gold + cache only (EDD §17, OOMKill guard) ───────────
    # bronze/silver NOT NULL scans on 10M rows exceed Trino container memory.
    # Non-blocking: pipeline proceeds even if quality issues are found.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test {DBT_FLAGS} --select gold cache --threads 1 || true",
        execution_timeout=timedelta(minutes=30),
    )

    # ── 8. Row-count validation ───────────────────────────────────────────────
    validate = PythonOperator(
        task_id="validate_row_counts",
        python_callable=_validate_row_counts,
    )

    # ── Task order ────────────────────────────────────────────────────────────
    # Bronze → Silver → Gold (sequential within each layer).
    # Three cache chains run in parallel from gold_rebuild, Iceberg before MySQL.
    # dbt_test and validate run after all MySQL mirrors complete.
    (log_params >> dbt_clean >> dbt_deps >> tg_bronze >> tg_silver >> tg_gold)

    tg_gold >> dbt_cache_daily_iceberg >> dbt_cache_daily_mysql
    tg_gold >> dbt_cache_hourly_iceberg >> dbt_cache_hourly_mysql
    tg_gold >> dbt_cache_report_iceberg >> dbt_cache_report_mysql

    [dbt_cache_daily_mysql, dbt_cache_hourly_mysql, dbt_cache_report_mysql] >> dbt_test >> validate
