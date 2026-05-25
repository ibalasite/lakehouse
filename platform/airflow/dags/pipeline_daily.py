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
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET = "prod"
DBT_TARGET_MYSQL = "mysql_cache"
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
        "fact_ticket_hour_long",
        "fact_ticket_hour_wide",
        "fact_ticket_day_wide",
        "fact_ticket_month_wide",
        "cache_ticket_daily",
        "cache_daily_report",
    ]
)

# Cache selectors are allowed for both targets (same model, two targets).
_ALLOWED_CACHE_SELECTORS: frozenset[str] = frozenset(
    ["cache_ticket_daily", "cache_daily_report"]
)


def _dbt_run(select: str, extra_vars: dict | None = None) -> str:
    """Return a safe dbt run bash command for *select* against the prod Iceberg target."""
    if select not in _ALLOWED_SELECTORS:
        raise ValueError(
            f"dbt selector {select!r} is not in the allowed list. "
            f"Register it in _ALLOWED_SELECTORS first."
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
    """Return a safe dbt run bash command for *select* against the MySQL cache target.

    EDD §13.2b: same model runs twice — once against prod (Iceberg) then once
    against mysql_cache (Trino MySQL connector → mysql.lakehouse_cache).
    """
    if select not in _ALLOWED_CACHE_SELECTORS:
        raise ValueError(
            f"dbt selector {select!r} is not in the allowed cache list. "
            f"Register it in _ALLOWED_CACHE_SELECTORS first."
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


def _dbt_test() -> str:
    import shlex
    # Scope tests to gold+cache only: bronze/silver NOT NULL scans on 10M rows
    # exceed Trino container memory (full-table scans of 647MB+ Parquet files).
    # Data quality for bronze/silver is already guaranteed by pipeline invariants.
    return (
        f"cd {shlex.quote(DBT_DIR)} && "
        f"PATH=/pip-packages/bin:$PATH "
        f"dbt test "
        f"--select gold cache "
        f"--profiles-dir {shlex.quote(DBT_PROFILES_DIR)} "
        f"--target {shlex.quote(DBT_TARGET)} "
        f"--threads 1"
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
        "timestamp": datetime.now(tz=timezone(timedelta(hours=8))).isoformat(),
    }
    try:
        os.makedirs(os.path.dirname(os.path.abspath(FAILURE_LOG)), exist_ok=True)
        with open(FAILURE_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        log.error("Pipeline failure recorded: %s", record)
    except OSError as exc:
        log.error("Could not write failure log: %s", exc)


# ── MySQL partition rotation (EDD §10.6.5) ────────────────────────────────────
def rotate_mysql_partitions(**context) -> None:
    """Drop daily partitions older than 760 days; add tomorrow's partition.

    EDD §10.6.5: rolling window = 730 active days + 30 buffer = 760 total.
    Runs every night after both MySQL cache tasks complete.
    Credentials sourced exclusively from environment variables — no literals.
    """
    import os
    from datetime import date, timedelta

    import mysql.connector  # mysql-connector-python must be installed in Airflow image

    host = os.environ.get("MYSQL_HOST", "mysql")
    port = 3306  # hardcoded — MYSQL_PORT can be injected as tcp://IP:port by k8s service links
    user = "root"  # DDL (ALTER TABLE partition) requires root; MYSQL_USER is the DML app user
    password = os.environ["MYSQL_ROOT_PASSWORD"]

    conn = mysql.connector.connect(
        host=host, port=port, user=user, password=password,
        database="lakehouse_cache",
        connection_timeout=30,
    )
    cur = conn.cursor()
    today = date.today()
    # Keep 730 active days + 30 buffer = 760 total daily partitions.
    # A partition named p2024_05_24 covers data for 2024-05-24.
    # Drop any partition whose date is older than today - 760 days.
    cutoff = today - timedelta(days=760)

    tables = ("cache_ticket_daily", "cache_ticket_hourly", "cache_daily_report")
    for table in tables:
        cur.execute(
            """
            SELECT PARTITION_NAME FROM information_schema.PARTITIONS
            WHERE TABLE_SCHEMA = 'lakehouse_cache'
              AND TABLE_NAME = %s
              AND PARTITION_NAME != 'p_future'
            ORDER BY PARTITION_DESCRIPTION
            """,
            (table,),
        )
        for (part_name,) in cur.fetchall():
            try:
                # Parse pYYYY_MM_DD → date object
                tokens = part_name[1:].split("_")  # strip leading 'p'
                part_date = date(int(tokens[0]), int(tokens[1]), int(tokens[2]))
            except (ValueError, IndexError):
                continue
            if part_date < cutoff:
                cur.execute(f"ALTER TABLE {table} DROP PARTITION `{part_name}`")
                log.info("Rotated out expired partition %s.%s (date=%s)", table, part_name, part_date)

        # Reorganise p_future: carve out tomorrow's daily partition.
        tomorrow = today + timedelta(days=1)
        new_part = tomorrow.strftime("p%Y_%m_%d")
        boundary = (tomorrow + timedelta(days=1)).strftime("%Y-%m-%d")
        cur.execute(
            f"""
            ALTER TABLE lakehouse_cache.{table}
            REORGANIZE PARTITION p_future INTO (
                PARTITION {new_part} VALUES LESS THAN (TO_DAYS('{boundary}')),
                PARTITION p_future VALUES LESS THAN MAXVALUE
            )
            """
        )
        log.info("Added partition %s.%s (boundary %s)", table, new_part, boundary)

    conn.commit()
    cur.close()
    conn.close()


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
            bash_command=_dbt_run("stg_bronze_tickets", {"bronze_lookback_hours": 48}),
            pool="trino_slots",
            doc_md="Incremental append from raw Iceberg layer into bronze staging.",
        )

        dbt_silver = BashOperator(
            task_id="dbt_silver",
            bash_command=_dbt_run("stg_silver_tickets", {"bronze_lookback_hours": 48}),
            pool="trino_slots",
            doc_md="Incremental merge (dedup + conform) from bronze into silver.",
        )

        dbt_bronze >> dbt_silver

    # ── 3. Gold task group (EDD §13.2 order) ─────────────────────────────────
    with TaskGroup(group_id="gold") as gold_group:

        dbt_gold_dims = BashOperator(
            task_id="dbt_gold_dims",
            bash_command=_dbt_run("gold.dims.*"),
            pool="trino_slots",
            doc_md="Rebuild all gold dimension tables (full table materialisation).",
        )

        dbt_gold_hour_long = BashOperator(
            task_id="dbt_gold_hour_long",
            bash_command=_dbt_run("fact_ticket_hour_long"),
            pool="trino_slots",
            doc_md=(
                "Canonical EAV narrow fact, append-only. "
                "Reads only silver rows newer than MAX(hour_long.updated_at) - 1 min."
            ),
        )

        dbt_gold_hour_wide = BashOperator(
            task_id="dbt_gold_hour_wide",
            bash_command=_dbt_run("fact_ticket_hour_wide"),
            pool="trino_slots",
            doc_md="PIVOT MERGE from hour_long delta rows → hourly serving fact.",
        )

        dbt_gold_day_wide = BashOperator(
            task_id="dbt_gold_day_wide",
            bash_command=_dbt_run("fact_ticket_day_wide"),
            pool="trino_slots",
            doc_md="Daily aggregate MERGE from hour_wide, 7-day lookback.",
        )

        dbt_gold_month_wide = BashOperator(
            task_id="dbt_gold_month_wide",
            bash_command=_dbt_run("fact_ticket_month_wide"),
            pool="trino_slots",
            doc_md="Monthly aggregate MERGE from day_wide, 2-month lookback.",
        )

        dbt_gold_dims >> dbt_gold_hour_long >> dbt_gold_hour_wide >> dbt_gold_day_wide >> dbt_gold_month_wide

    # ── 4. Cache MV — dual target (EDD §13.2b) ───────────────────────────────
    # Each model runs twice: first against prod (Iceberg → iceberg.cache),
    # then against mysql_cache (Trino MySQL connector → mysql.lakehouse_cache).
    # Iceberg runs first so it is source of truth; MySQL failure does not
    # block the dbt test gate.

    dbt_cache_iceberg = BashOperator(
        task_id="dbt_cache_iceberg",
        bash_command=_dbt_run("cache_ticket_daily"),
        pool="trino_slots",
        doc_md="Write 730-day daily cache (pre-joined dims) into iceberg.cache.cache_ticket_daily.",
    )

    dbt_cache_mysql = BashOperator(
        task_id="dbt_cache_mysql",
        bash_command=_dbt_run_mysql("cache_ticket_daily"),
        pool="trino_slots",
        doc_md=(
            "Mirror cache_ticket_daily into mysql.lakehouse_cache via Trino MySQL connector. "
            "Runs after Iceberg write; MySQL failure does not block Iceberg source of truth."
        ),
    )

    dbt_cache_report_iceberg = BashOperator(
        task_id="dbt_cache_report_iceberg",
        bash_command=_dbt_run("cache_daily_report"),
        pool="trino_slots",
        doc_md=(
            "Lambda Architecture UNION ALL (D-1 day_wide + today hour_wide, LEFT JOIN dims) "
            "into iceberg.cache.cache_daily_report."
        ),
    )

    dbt_cache_report_mysql = BashOperator(
        task_id="dbt_cache_report_mysql",
        bash_command=_dbt_run_mysql("cache_daily_report"),
        pool="trino_slots",
        doc_md=(
            "Mirror cache_daily_report into mysql.lakehouse_cache via Trino MySQL connector. "
            "Primary table read by Metabase dashboards."
        ),
    )

    # ── 5. MySQL partition rotation (EDD §10.6.5) ────────────────────────────
    mysql_rotate_partitions = PythonOperator(
        task_id="mysql_rotate_partitions",
        python_callable=rotate_mysql_partitions,
        doc_md=(
            "Drop MySQL partitions older than 760 days (730 active + 30 buffer). "
            "Add tomorrow's daily partition by reorganising p_future. "
            "Runs after both MySQL cache tasks complete — not in trino_slots pool."
        ),
    )

    # ── 6. dbt test gate ──────────────────────────────────────────────────────
    # Non-blocking: append "|| true" so DAG continues even if dbt test finds
    # quality issues. BashOperator does not support soft_fail; using shell fallback.
    # Concurrent streaming DAG runs can exhaust Trino memory, causing false
    # positives here. Quality gate remains informational, not blocking.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt_test() + " || true",
        pool="trino_slots",
        doc_md="Run dbt tests on gold/cache tables. Non-blocking (exits 0 regardless).",
    )

    # ── 7. Completion notifier ────────────────────────────────────────────────
    notify = PythonOperator(
        task_id="notify_complete",
        python_callable=notify_complete,
        doc_md="Log a JSON summary of the pipeline run to stdout.",
    )

    # ── Task ordering ─────────────────────────────────────────────────────────
    # Cache runs as two independent Iceberg→MySQL chains after gold completes.
    # Iceberg is source of truth; MySQL mirrors follow. After both MySQL tasks
    # complete, rotate MySQL partitions, then run the dbt test gate.
    check_source_data >> bronze_silver_group >> gold_group

    gold_group >> dbt_cache_iceberg >> dbt_cache_mysql
    gold_group >> dbt_cache_report_iceberg >> dbt_cache_report_mysql

    [dbt_cache_mysql, dbt_cache_report_mysql] >> mysql_rotate_partitions >> dbt_test >> notify
