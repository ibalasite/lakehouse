#!/usr/bin/env python3
"""
Bulk-loads iceberg.bronze.stg_bronze_tickets from raw_tickets in 5 year-partitioned
batches to avoid Trino OOM on the initial full-table CTAS.

Strategy: CTAS for year 2022 (creates table), then INSERT INTO for 2023–2026.
Each batch processes ~2M rows instead of all 10M at once.

Run this once before the first lakehouse_daily DAG run.
"""
import os
import sys
import time
import urllib.request

import trino

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

TRANSFORM_SQL = """\
WITH source AS (
  SELECT *
  FROM iceberg.bronze.raw_tickets
  WHERE year(prblm_sysdate) = {year}
),
with_durations AS (
  SELECT
    *,
    CASE
      WHEN prblm_donedate IS NOT NULL
        THEN date_diff('hour', prblm_sysdate, prblm_donedate)
      ELSE NULL
    END AS _resolution_hours_raw,
    CASE
      WHEN prblm_preassignenddate IS NOT NULL
        THEN date_diff('hour', prblm_sysdate, prblm_preassignenddate)
      ELSE NULL
    END AS _response_hours_raw
  FROM source
)
SELECT
  prblm_code,
  prblm_sysdate,
  prblm_updateddate,
  prblm_donedate,
  prblm_preassignenddate,
  prblm_preassignbegindate,
  prblm_status_id,
  prblm_source_id,
  prblm_class_id,
  prblm_intclass_id,
  prblm_perform_id,
  prblm_complain_id,
  prblm_processuser,
  prblm_doneuser,
  prblm_sysuser,
  CASE
    WHEN prblm_name IS NULL THEN NULL
    WHEN length(prblm_name) = 0 THEN ''
    ELSE concat(substr(prblm_name, 1, 1), '***')
  END AS customer_name_masked,
  bitwise_and(
    from_big_endian_64(xxhash64(to_utf8(cast(
      coalesce(cast('pii:' || coalesce(cast(usr_id AS varchar), '__null__') AS varchar), '__null__')
    AS varchar)))),
    9223372036854775807
  ) AS usr_pk,
  CASE
    WHEN usr_id IS NULL           THEN NULL
    WHEN length(usr_id) <= 4      THEN '***'
    ELSE concat(substr(usr_id, 1, 2), '***', substr(usr_id, length(usr_id) - 1, 2))
  END AS usr_id_masked,
  gd_id,
  catsub_id,
  supplier_id,
  prblm_doneatatime,
  prblm_forwarddatetime,
  prblm_forwardtype,
  prblm_notasreason_id,
  prblm_notasowndept_id,
  _resolution_hours_raw AS resolution_hours,
  _response_hours_raw   AS response_hours,
  CASE
    WHEN _resolution_hours_raw IS NOT NULL AND _resolution_hours_raw >= 0 THEN 1
    ELSE 0
  END AS is_resolved,
  CASE
    WHEN _response_hours_raw IS NULL THEN 0
    WHEN prblm_perform_id = 2    AND _response_hours_raw <= 4  THEN 1
    WHEN prblm_perform_id = 1    AND _response_hours_raw <= 8  THEN 1
    WHEN prblm_perform_id IS NULL AND _response_hours_raw <= 24 THEN 1
    ELSE 0
  END AS within_sla,
  ingested_at,
  current_timestamp AS bronze_created_at
FROM with_durations\
"""


def wait_for_trino(host: str, port: int, retries: int = 60) -> None:
    url = f"http://{host}:{port}/v1/info"
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status == 200:
                    print(f"Trino ready ({url})")
                    return
        except Exception as exc:
            print(f"Waiting for Trino ({attempt}/{retries}): {exc}")
            time.sleep(10)
    raise RuntimeError(f"Trino not ready after {retries} attempts")


def get_conn() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="bulk-loader",
        http_scheme="http",
        request_timeout=900,  # 15 min per HTTP poll
    )


def run_ddl(conn: trino.dbapi.Connection, sql: str, desc: str) -> None:
    print(f"\n{'─'*60}")
    print(f"  {desc}")
    print(f"{'─'*60}")
    t0 = time.time()
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchone()
    elapsed = time.time() - t0
    print(f"  Done in {elapsed:.1f}s — {result}")


def table_row_count(conn: trino.dbapi.Connection) -> int:
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM iceberg.bronze.stg_bronze_tickets")
    return cur.fetchone()[0]


def main() -> None:
    wait_for_trino(TRINO_HOST, TRINO_PORT)

    conn = get_conn()

    # Idempotency check: skip if already populated
    try:
        count = table_row_count(conn)
        if count > 0:
            print(f"stg_bronze_tickets already has {count:,} rows — nothing to do.")
            return
        print("stg_bronze_tickets exists but is empty — will populate.")
        table_exists = True
    except Exception:
        print("stg_bronze_tickets does not exist — will create and populate.")
        table_exists = False

    years = [2022, 2023, 2024, 2025, 2026]

    for i, year in enumerate(years):
        body = TRANSFORM_SQL.format(year=year)
        if i == 0 and not table_exists:
            sql = (
                "CREATE TABLE iceberg.bronze.stg_bronze_tickets "
                "WITH (format = 'PARQUET') AS\n" + body
            )
            run_ddl(conn, sql, f"CTAS  year={year}")
        else:
            sql = "INSERT INTO iceberg.bronze.stg_bronze_tickets\n" + body
            run_ddl(conn, sql, f"INSERT year={year}")

    total = table_row_count(conn)
    print(f"\n{'='*60}")
    print(f"  Bulk load complete: {total:,} rows in iceberg.bronze.stg_bronze_tickets")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
