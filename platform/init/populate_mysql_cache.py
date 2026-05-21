#!/usr/bin/env python3
"""
populate_mysql_cache.py
=======================
Reads aggregated metrics from Trino (Iceberg) and bulk-inserts into MySQL
cache tables for Metabase consumption.

Usage
-----
    python3 populate_mysql_cache.py          # full backfill
    python3 populate_mysql_cache.py --daily-only
    python3 populate_mysql_cache.py --hourly-only
    python3 populate_mysql_cache.py --streaming  # today only, from gold layer (streaming DAG)

Speed: uses executemany with 5k-row batches — typically < 30s for full backfill.
--streaming runs in < 2s: reads today's pre-aggregated rows from fact_ticket_hour_wide.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import mysql.connector
import trino.dbapi

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("cache_populate")


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        log.error("Required environment variable %s is not set. Run: source .env", name)
        sys.exit(1)
    return value


# ── Config ─────────────────────────────────────────────────────────────────────
TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
_mysql_port_raw = os.environ.get("MYSQL_PORT", "3306")
MYSQL_PORT = int(_mysql_port_raw.split(":")[-1]) if "://" in _mysql_port_raw else int(_mysql_port_raw)
MYSQL_USER = os.environ.get("MYSQL_USER", "lakehouse")
MYSQL_PASS = _require_env("MYSQL_PASSWORD")
MYSQL_DB   = os.environ.get("MYSQL_DATABASE", "lakehouse_cache")

BATCH_SIZE = 5_000

# ── Trino aggregation queries ──────────────────────────────────────────────────
DAILY_QUERY_MONTH = """
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
WHERE prblm_sysdate >= TIMESTAMP '{year}-{month:02d}-01 00:00:00.000000 UTC'
  AND prblm_sysdate <  TIMESTAMP '{next_year}-{next_month:02d}-01 00:00:00.000000 UTC'
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2
"""

HOURLY_QUERY_MONTH = """
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
WHERE prblm_sysdate >= TIMESTAMP '{year}-{month:02d}-01 00:00:00.000000 UTC'
  AND prblm_sysdate <  TIMESTAMP '{next_year}-{next_month:02d}-01 00:00:00.000000 UTC'
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1,2,3
"""

DAILY_INSERT = """
INSERT INTO cache_ticket_daily
    (date_sk, catsub_id, prblm_source_id, prblm_class_id,
     prblm_perform_id, prblm_status_id,
     total_tickets, resolved_tickets, one_shot_resolved,
     complain_tickets, forwarded_tickets, within_sla_tickets,
     avg_resolution_hours, avg_response_hours,
     pct_resolved, pct_within_sla, pct_one_shot)
VALUES
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

HOURLY_INSERT = """
INSERT INTO cache_ticket_hourly
    (date_sk, hour_of_day, catsub_id, prblm_source_id, prblm_class_id,
     prblm_perform_id, prblm_status_id,
     total_tickets, resolved_tickets, one_shot_resolved,
     complain_tickets, forwarded_tickets, within_sla_tickets,
     avg_resolution_hours, avg_response_hours)
VALUES
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

# Streaming mode: aggregate today's silver rows directly.
# Bypasses the gold MERGE entirely — the daily DAG handles the gold rebuild.
# Silver today = at most a few hundred rows → trivial Trino query, no OOM risk.
HOURLY_STREAMING_QUERY = """
SELECT
    CAST(prblm_sysdate AT TIME ZONE 'Asia/Taipei' AS DATE)       AS date_sk,
    CAST(EXTRACT(HOUR FROM (prblm_sysdate AT TIME ZONE 'Asia/Taipei')) AS INTEGER)
                                                                  AS hour_of_day,
    catsub_id,
    prblm_source_id,
    prblm_class_id,
    COALESCE(prblm_perform_id, 99)                                AS prblm_perform_id,
    prblm_status_id,
    COUNT(*)                                                      AS total_tickets,
    SUM(is_resolved)                                              AS resolved_tickets,
    SUM(CASE WHEN prblm_doneatatime THEN 1 ELSE 0 END)            AS one_shot_resolved,
    SUM(is_complain)                                              AS complain_tickets,
    SUM(is_forwarded)                                             AS forwarded_tickets,
    SUM(within_sla)                                               AS within_sla_tickets,
    AVG(CASE WHEN resolution_hours >= 0
             THEN CAST(resolution_hours AS DOUBLE) END)           AS avg_resolution_hours,
    AVG(CASE WHEN response_hours >= 0
             THEN CAST(response_hours AS DOUBLE) END)             AS avg_response_hours
FROM iceberg.silver.stg_silver_tickets
WHERE prblm_date = CAST(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Taipei' AS DATE)
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2
"""


def _months() -> list[tuple[int, int]]:
    """Return list of (year, month) from 2022-01 to 2026-05."""
    result = []
    for y in range(2022, 2027):
        for m in range(1, 13):
            if (y, m) > (2026, 5):
                break
            result.append((y, m))
    return result


def _next_month(y: int, m: int) -> tuple[int, int]:
    return (y + 1, 1) if m == 12 else (y, m + 1)


def get_trino(schema: str = "bronze") -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="admin",
        catalog="iceberg", schema=schema,
        http_scheme="http", request_timeout=120,
    )


def get_mysql() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASS,
        database=MYSQL_DB,
        autocommit=False,
    )


def _insert_daily_rows(rows_raw: list, mc, mysql_conn, t_insert: float, total_inserted: list) -> None:
    def row_daily(r):
        total = r[6] or 1
        return (r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11],r[12],r[13],
                round(r[7]/total*100,2), round(r[11]/total*100,2), round(r[8]/total*100,2))
    rows = [row_daily(r) for r in rows_raw]
    for i in range(0, len(rows), BATCH_SIZE):
        mc.executemany(DAILY_INSERT, rows[i:i+BATCH_SIZE])
        mysql_conn.commit()
    total_inserted[0] += len(rows)


def populate_daily(mysql_conn) -> int:
    log.info("Populating cache_ticket_daily month-by-month…")
    mc = mysql_conn.cursor()
    mc.execute("DELETE FROM cache_ticket_daily")
    mysql_conn.commit()

    total_inserted = [0]
    t0 = time.monotonic()

    for year, month in _months():
        ny, nm = _next_month(year, month)
        sql = DAILY_QUERY_MONTH.format(year=year, month=month, next_year=ny, next_month=nm)

        # Fresh connection per month to avoid port-forward timeout
        conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="admin",
                                   catalog="iceberg", schema="bronze",
                                   http_scheme="http", request_timeout=120)
        cur = conn.cursor()
        cur.execute(sql)
        rows_raw = cur.fetchall()
        conn.close()

        if rows_raw:
            _insert_daily_rows(rows_raw, mc, mysql_conn, t0, total_inserted)
        elapsed = time.monotonic() - t0
        log.info("  %d-%02d: %s rows (total %s, %.0f rows/sec)",
                 year, month, f"{len(rows_raw):,}", f"{total_inserted[0]:,}",
                 total_inserted[0] / elapsed if elapsed else 0)

    log.info("cache_ticket_daily done: %s rows in %.1fs",
             f"{total_inserted[0]:,}", time.monotonic() - t0)
    return total_inserted[0]


def populate_hourly(mysql_conn) -> int:
    log.info("Populating cache_ticket_hourly month-by-month…")
    mc = mysql_conn.cursor()
    mc.execute("DELETE FROM cache_ticket_hourly")
    mysql_conn.commit()

    total_inserted = 0
    t0 = time.monotonic()

    for year, month in _months():
        ny, nm = _next_month(year, month)
        sql = HOURLY_QUERY_MONTH.format(year=year, month=month, next_year=ny, next_month=nm)

        conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="admin",
                                   catalog="iceberg", schema="bronze",
                                   http_scheme="http", request_timeout=120)
        cur = conn.cursor()
        cur.execute(sql)
        rows_raw = cur.fetchall()
        conn.close()

        if rows_raw:
            for i in range(0, len(rows_raw), BATCH_SIZE):
                mc.executemany(HOURLY_INSERT, rows_raw[i:i+BATCH_SIZE])
            mysql_conn.commit()
        total_inserted += len(rows_raw)
        elapsed = time.monotonic() - t0
        log.info("  %d-%02d: %s rows (total %s, %.0f rows/sec)",
                 year, month, f"{len(rows_raw):,}", f"{total_inserted:,}",
                 total_inserted / elapsed if elapsed else 0)

    log.info("cache_ticket_hourly done: %s rows in %.1fs",
             f"{total_inserted:,}", time.monotonic() - t0)
    return total_inserted


def populate_hourly_today(mysql_conn) -> int:
    """Refresh only today's hourly rows from the pre-aggregated gold layer.

    Used by the 15-min streaming DAG.  Reads from fact_ticket_hour_wide (already
    aggregated, < 1000 rows) instead of scanning 10M raw_tickets rows.
    """
    log.info("Streaming mode: refreshing today's hourly cache from gold layer…")
    mc = mysql_conn.cursor()
    mc.execute("DELETE FROM cache_ticket_hourly WHERE date_sk = CURDATE()")
    mysql_conn.commit()
    deleted = mc.rowcount
    log.info("  Deleted %d stale rows for today", deleted)

    t0 = time.monotonic()
    conn = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="admin",
        catalog="iceberg", schema="gold",
        http_scheme="http", request_timeout=60,
    )
    cur = conn.cursor()
    cur.execute(HOURLY_STREAMING_QUERY)
    rows_raw = cur.fetchall()
    conn.close()

    if rows_raw:
        mc.executemany(HOURLY_INSERT, rows_raw)
        mysql_conn.commit()

    log.info("  Inserted %d rows for today in %.1fs", len(rows_raw), time.monotonic() - t0)
    return len(rows_raw)


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate MySQL cache from Trino Iceberg")
    parser.add_argument("--daily-only",  action="store_true")
    parser.add_argument("--hourly-only", action="store_true")
    parser.add_argument("--streaming",   action="store_true",
                        help="Streaming mode: only refresh today's hourly data from gold layer")
    args = parser.parse_args()

    log.info("Connecting to MySQL at %s:%d…", MYSQL_HOST, MYSQL_PORT)
    mysql_conn = get_mysql()

    t0 = time.monotonic()

    if args.streaming:
        n = populate_hourly_today(mysql_conn)
        log.info("Streaming hourly cache: %s rows", f"{n:,}")
    else:
        do_daily  = not args.hourly_only
        do_hourly = not args.daily_only

        if do_daily:
            n = populate_daily(mysql_conn)
            log.info("Daily cache: %s rows", f"{n:,}")

        if do_hourly:
            n = populate_hourly(mysql_conn)
            log.info("Hourly cache: %s rows", f"{n:,}")

    log.info("Total time: %.1fs", time.monotonic() - t0)
    mysql_conn.close()


if __name__ == "__main__":
    main()
