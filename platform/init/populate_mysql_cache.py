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

Speed: uses executemany with 5k-row batches — typically < 30s for full backfill.
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
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
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
FROM iceberg.raw.raw_tickets
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
FROM iceberg.raw.raw_tickets
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


def get_trino() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="admin",
        catalog="iceberg", schema="raw",
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
                                   catalog="iceberg", schema="raw",
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
                                   catalog="iceberg", schema="raw",
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate MySQL cache from Trino Iceberg")
    parser.add_argument("--daily-only",  action="store_true")
    parser.add_argument("--hourly-only", action="store_true")
    args = parser.parse_args()

    do_daily  = not args.hourly_only
    do_hourly = not args.daily_only

    log.info("Connecting to MySQL at %s:%d…", MYSQL_HOST, MYSQL_PORT)
    mysql_conn = get_mysql()

    t0 = time.monotonic()

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
