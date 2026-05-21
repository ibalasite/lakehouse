#!/usr/bin/env python3
"""
Bulk-loads iceberg.silver.stg_silver_tickets from stg_bronze_tickets in 5
year-partitioned batches to avoid Trino EXCEEDED_LOCAL_MEMORY_LIMIT on the
initial full-table ROW_NUMBER() dedup over 10M rows.
"""
import os
import time
import urllib.request

import trino

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

TRANSFORM_SQL = """\
WITH bronze_batch AS (
  SELECT *
  FROM iceberg.bronze.stg_bronze_tickets
  WHERE year(prblm_sysdate) = {year}
),
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY prblm_code
      ORDER BY prblm_updateddate DESC NULLS LAST
    ) AS _row_rank
  FROM bronze_batch
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
  customer_name_masked,
  usr_pk,
  usr_id_masked,
  gd_id,
  catsub_id,
  supplier_id,
  prblm_doneatatime,
  prblm_forwarddatetime,
  prblm_forwardtype,
  prblm_notasreason_id,
  prblm_notasowndept_id,
  resolution_hours,
  response_hours,
  is_resolved,
  within_sla,
  CASE
    WHEN resolution_hours IS NULL OR resolution_hours < 0 THEN '未結案'
    WHEN resolution_hours <= 24  THEN '24hr內'
    WHEN resolution_hours <= 48  THEN '24~48hr'
    WHEN resolution_hours <= 72  THEN '48~72hr'
    WHEN resolution_hours <= 120 THEN '3~5天'
    WHEN resolution_hours <= 168 THEN '5~7天'
    WHEN resolution_hours <= 240 THEN '7~10天'
    WHEN resolution_hours <= 360 THEN '10~15天'
    ELSE                              '15天以上'
  END AS resolution_bucket,
  CASE
    WHEN response_hours IS NULL OR response_hours < 0 THEN '未回覆'
    WHEN response_hours <= 4   THEN '4hr內'
    WHEN response_hours <= 8   THEN '4~8hr'
    WHEN response_hours <= 12  THEN '8~12hr'
    WHEN response_hours <= 24  THEN '12~24hr'
    WHEN response_hours <= 48  THEN '24~48hr'
    WHEN response_hours <= 72  THEN '48~72hr'
    ELSE                            '72hr以上'
  END AS response_bucket,
  CASE WHEN prblm_complain_id >= 2 THEN 1 ELSE 0 END           AS is_complain,
  CASE WHEN prblm_forwarddatetime IS NOT NULL THEN 1 ELSE 0 END AS is_forwarded,
  CAST(prblm_sysdate AT TIME ZONE 'Asia/Taipei' AS DATE)        AS prblm_date,
  ingested_at,
  current_timestamp AS updated_at
FROM deduped
WHERE _row_rank = 1\
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
        request_timeout=900,
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
    cur.execute("SELECT count(*) FROM iceberg.silver.stg_silver_tickets")
    return cur.fetchone()[0]


def main() -> None:
    wait_for_trino(TRINO_HOST, TRINO_PORT)

    conn = get_conn()

    try:
        count = table_row_count(conn)
        if count > 0:
            print(f"stg_silver_tickets already has {count:,} rows — nothing to do.")
            return
        print("stg_silver_tickets exists but is empty — will populate.")
        table_exists = True
    except Exception:
        print("stg_silver_tickets does not exist — will create and populate.")
        table_exists = False

    years = [2022, 2023, 2024, 2025, 2026]

    for i, year in enumerate(years):
        body = TRANSFORM_SQL.format(year=year)
        if i == 0 and not table_exists:
            sql = (
                "CREATE TABLE iceberg.silver.stg_silver_tickets "
                "WITH (format = 'PARQUET') AS\n" + body
            )
            run_ddl(conn, sql, f"CTAS  year={year}")
        else:
            sql = "INSERT INTO iceberg.silver.stg_silver_tickets\n" + body
            run_ddl(conn, sql, f"INSERT year={year}")

    total = table_row_count(conn)
    print(f"\n{'='*60}")
    print(f"  Bulk load complete: {total:,} rows in iceberg.silver.stg_silver_tickets")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
