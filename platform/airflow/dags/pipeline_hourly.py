"""
pipeline_hourly.py
==================
Airflow DAG: lakehouse_hourly

Purpose : Refresh TODAY's MySQL cache (daily + hourly) from Trino Iceberg,
          then verify the Metabase dashboard card returns data.

Trigger : Manual only — invoked by e2e_smoke_test.py or the Airflow UI.
Schedule: None (no auto-run)

Task graph:
    refresh_daily >> refresh_hourly >> verify_dashboard
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

# ── Bash task bodies ──────────────────────────────────────────────────────────
# NOTE: outer string uses triple-double-quotes; inner SQL uses triple-single-quotes
# to avoid premature string termination.

_REFRESH_DAILY_CMD = r"""
set -e
pip install --quiet "mysql-connector-python>=9.0" "trino>=0.330" 2>/dev/null

python3 - <<'PYEOF'
import os, sys, time, logging
import mysql.connector
import trino.dbapi

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("refresh_daily")

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_PORT = 3306
MYSQL_USER = os.environ.get("MYSQL_USER", "lakehouse")
MYSQL_PASS = os.environ.get("MYSQL_PASSWORD", "")
MYSQL_DB   = os.environ.get("MYSQL_DATABASE", "lakehouse_cache")

DAILY_QUERY = '''
SELECT
    CAST(DATE_TRUNC('day', prblm_sysdate) AS DATE)  AS date_sk,
    catsub_id,
    prblm_source_id,
    prblm_class_id,
    prblm_perform_id,
    prblm_status_id,
    COUNT(*)                                         AS total_tickets,
    COUNT(*) FILTER (WHERE prblm_status_id = 5)      AS resolved_tickets,
    COUNT(*) FILTER (WHERE prblm_doneatatime = TRUE)  AS one_shot_resolved,
    COUNT(*) FILTER (WHERE prblm_complain_id >= 2)   AS complain_tickets,
    COUNT(*) FILTER (WHERE prblm_forwardtype IS NOT NULL) AS forwarded_tickets,
    COUNT(*) FILTER (WHERE prblm_doneatatime = TRUE
                       AND prblm_status_id = 5)      AS within_sla_tickets,
    AVG(CASE WHEN prblm_donedate IS NOT NULL
             THEN DATE_DIFF('minute', prblm_sysdate, prblm_donedate) / 60.0
             END)                                    AS avg_resolution_hours,
    AVG(DATE_DIFF('minute', prblm_sysdate, prblm_preassignenddate) / 60.0)
                                                     AS avg_response_hours
FROM iceberg.bronze.raw_tickets
WHERE CAST(DATE_TRUNC('day', prblm_sysdate) AS DATE) = CURRENT_DATE
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2
'''

DAILY_INSERT = (
    "INSERT INTO cache_ticket_daily "
    "(date_sk, catsub_id, prblm_source_id, prblm_class_id, "
    " prblm_perform_id, prblm_status_id, "
    " total_tickets, resolved_tickets, one_shot_resolved, "
    " complain_tickets, forwarded_tickets, within_sla_tickets, "
    " avg_resolution_hours, avg_response_hours, "
    " pct_resolved, pct_within_sla, pct_one_shot) "
    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
)

t0 = time.monotonic()
conn_t = trino.dbapi.connect(
    host=TRINO_HOST, port=TRINO_PORT, user="admin",
    catalog="iceberg", schema="raw", http_scheme="http", request_timeout=120,
)
cur = conn_t.cursor()
cur.execute(DAILY_QUERY)
rows_raw = cur.fetchall()
conn_t.close()
log.info("Trino daily query: %d rows for today", len(rows_raw))

if not rows_raw:
    log.warning("No rows for today — daily cache unchanged")
    sys.exit(0)

def row_daily(r):
    total = r[6] or 1
    return (r[0],r[1],r[2],r[3],r[4],r[5],
            r[6],r[7],r[8],r[9],r[10],r[11],r[12],r[13],
            round(r[7]/total*100,2), round(r[11]/total*100,2), round(r[8]/total*100,2))

rows = [row_daily(r) for r in rows_raw]

conn_m = mysql.connector.connect(
    host=MYSQL_HOST, port=MYSQL_PORT,
    user=MYSQL_USER, password=MYSQL_PASS, database=MYSQL_DB, autocommit=False,
)
mc = conn_m.cursor()
mc.execute("DELETE FROM cache_ticket_daily WHERE date_sk = CURDATE()")
conn_m.commit()
for i in range(0, len(rows), 5000):
    mc.executemany(DAILY_INSERT, rows[i:i+5000])
conn_m.commit()
conn_m.close()
log.info("Daily cache refreshed: %d rows in %.1fs", len(rows), time.monotonic() - t0)
PYEOF
"""

_REFRESH_HOURLY_CMD = r"""
set -e
pip install --quiet "mysql-connector-python>=9.0" "trino>=0.330" 2>/dev/null

python3 - <<'PYEOF'
import os, sys, time, logging
import mysql.connector
import trino.dbapi

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("refresh_hourly")

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_PORT = 3306
MYSQL_USER = os.environ.get("MYSQL_USER", "lakehouse")
MYSQL_PASS = os.environ.get("MYSQL_PASSWORD", "")
MYSQL_DB   = os.environ.get("MYSQL_DATABASE", "lakehouse_cache")

HOURLY_QUERY = '''
SELECT
    CAST(DATE_TRUNC('day',  prblm_sysdate) AS DATE)  AS date_sk,
    CAST(HOUR(prblm_sysdate) AS INTEGER)              AS hour_of_day,
    catsub_id,
    prblm_source_id,
    prblm_class_id,
    prblm_perform_id,
    prblm_status_id,
    COUNT(*)                                          AS total_tickets,
    COUNT(*) FILTER (WHERE prblm_status_id = 5)       AS resolved_tickets,
    COUNT(*) FILTER (WHERE prblm_doneatatime = TRUE)   AS one_shot_resolved,
    COUNT(*) FILTER (WHERE prblm_complain_id >= 2)    AS complain_tickets,
    COUNT(*) FILTER (WHERE prblm_forwardtype IS NOT NULL) AS forwarded_tickets,
    COUNT(*) FILTER (WHERE prblm_doneatatime = TRUE
                        AND prblm_status_id = 5)      AS within_sla_tickets,
    AVG(CASE WHEN prblm_donedate IS NOT NULL
             THEN DATE_DIFF('minute', prblm_sysdate, prblm_donedate) / 60.0
             END)                                     AS avg_resolution_hours,
    AVG(DATE_DIFF('minute', prblm_sysdate, prblm_preassignenddate) / 60.0)
                                                      AS avg_response_hours
FROM iceberg.bronze.raw_tickets
WHERE CAST(DATE_TRUNC('day', prblm_sysdate) AS DATE) = CURRENT_DATE
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1,2,3
'''

HOURLY_INSERT = (
    "INSERT INTO cache_ticket_hourly "
    "(date_sk, hour_of_day, catsub_id, prblm_source_id, prblm_class_id, "
    " prblm_perform_id, prblm_status_id, "
    " total_tickets, resolved_tickets, one_shot_resolved, "
    " complain_tickets, forwarded_tickets, within_sla_tickets, "
    " avg_resolution_hours, avg_response_hours) "
    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
)

t0 = time.monotonic()
conn_t = trino.dbapi.connect(
    host=TRINO_HOST, port=TRINO_PORT, user="admin",
    catalog="iceberg", schema="raw", http_scheme="http", request_timeout=120,
)
cur = conn_t.cursor()
cur.execute(HOURLY_QUERY)
rows = cur.fetchall()
conn_t.close()
log.info("Trino hourly query: %d rows for today", len(rows))

if not rows:
    log.warning("No rows for today — hourly cache unchanged")
    sys.exit(0)

conn_m = mysql.connector.connect(
    host=MYSQL_HOST, port=MYSQL_PORT,
    user=MYSQL_USER, password=MYSQL_PASS, database=MYSQL_DB, autocommit=False,
)
mc = conn_m.cursor()
mc.execute("DELETE FROM cache_ticket_hourly WHERE date_sk = CURDATE()")
conn_m.commit()
for i in range(0, len(rows), 5000):
    mc.executemany(HOURLY_INSERT, rows[i:i+5000])
conn_m.commit()
conn_m.close()
log.info("Hourly cache refreshed: %d rows in %.1fs", len(rows), time.monotonic() - t0)
PYEOF
"""

_VERIFY_DASHBOARD_CMD = r"""
set -e
pip install --quiet requests 2>/dev/null

python3 - <<'PYEOF'
import os, sys, logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("verify_dashboard")

MB_URL   = os.environ.get("METABASE_URL", "http://metabase:3000")
MB_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@local.com")
MB_PASS  = os.environ.get("METABASE_ADMIN_PASSWORD", "")

r = requests.post(f"{MB_URL}/api/session",
                  json={"username": MB_EMAIL, "password": MB_PASS}, timeout=30)
if r.status_code != 200:
    log.error("Metabase login failed: %d %s", r.status_code, r.text[:200])
    sys.exit(1)
token = r.json()["id"]
hdrs = {"X-Metabase-Session": token}

r2 = requests.get(f"{MB_URL}/api/dashboard/2", headers=hdrs, timeout=30)
if r2.status_code != 200:
    log.error("Dashboard fetch failed: %d", r2.status_code)
    sys.exit(1)

dash = r2.json()
dashcards = dash.get("dashcards", [])
log.info("Dashboard '%s' has %d card(s)", dash.get("name"), len(dashcards))

if not dashcards:
    log.error("Dashboard has 0 cards!")
    sys.exit(1)

total = 0
for dc in dashcards[:3]:
    cid = dc.get("card_id")
    if not cid:
        continue
    r3 = requests.post(f"{MB_URL}/api/card/{cid}/query", headers=hdrs, timeout=60)
    if r3.status_code in (200, 202):
        rows = r3.json().get("data", {}).get("rows", [])
        total += len(rows)
        log.info("  Card %d -> %d rows", cid, len(rows))
    else:
        log.warning("  Card %d query: HTTP %d", cid, r3.status_code)

if total == 0:
    log.error("All queried cards returned 0 rows!")
    sys.exit(1)
log.info("Dashboard verification passed: %d total rows across cards", total)
PYEOF
"""

# ── DAG definition ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="lakehouse_hourly",
    description="Refresh today's MySQL cache from Trino and verify Metabase",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "smoke", "hourly"],
    doc_md="""
### lakehouse_hourly

Manual trigger only. Refreshes MySQL cache for TODAY's data from Trino Iceberg,
then verifies the Metabase dashboard returns rows.

Triggered by `e2e_smoke_test.py` after writing new hourly ticket data into Iceberg.

Task graph: `refresh_daily >> refresh_hourly >> verify_dashboard`
""",
) as dag:

    refresh_daily = BashOperator(
        task_id="refresh_daily",
        bash_command=_REFRESH_DAILY_CMD,
        execution_timeout=timedelta(minutes=10),
        doc_md="Delete and re-insert cache_ticket_daily for CURRENT_DATE from Trino.",
    )

    refresh_hourly = BashOperator(
        task_id="refresh_hourly",
        bash_command=_REFRESH_HOURLY_CMD,
        execution_timeout=timedelta(minutes=10),
        doc_md="Delete and re-insert cache_ticket_hourly for CURRENT_DATE from Trino.",
    )

    verify_dashboard = BashOperator(
        task_id="verify_dashboard",
        bash_command=_VERIFY_DASHBOARD_CMD,
        execution_timeout=timedelta(minutes=5),
        doc_md="Log into Metabase and confirm the dashboard card returns data.",
    )

    refresh_daily >> refresh_hourly >> verify_dashboard
