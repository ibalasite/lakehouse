#!/usr/bin/env python3
"""
04_generate_tickets.py
======================
Generates synthetic e-commerce customer-service ticket records and writes them
directly to the Iceberg lakehouse via the Trino Python client.

Usage
-----
    python 04_generate_tickets.py                           # full 10M run
    python 04_generate_tickets.py --total-rows 100000       # quick test
    python 04_generate_tickets.py --dry-run                 # validate only
    python 04_generate_tickets.py --host trino --port 8080  # custom endpoint

Design choices
--------------
* Pure numpy arrays for data generation — no Faker loops, 50-100x faster.
* Iceberg table created with PARQUET format, partitioned by year(prblm_sysdate).
* INSERT batches use UNION ALL style to avoid prepared-statement overhead with
  the Trino HTTP transport.  Each batch is a single SQL string; 50 k rows
  splits into sub-chunks of 1 000 rows each to stay well under Trino's
  max-query-length limit (~16 MB).
* Idempotent: the table is created with CREATE TABLE IF NOT EXISTS; rerunning
  appends new rows (safe for backfill).  Use --truncate to clear first.
* Retry logic: exponential back-off up to 5 attempts per batch.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import numpy as np
import trino.dbapi

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None  # type: ignore[misc]

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("ticket_gen")

# ── Name pools ────────────────────────────────────────────────────────────────
SURNAMES: list[str] = [
    "王", "李", "張", "劉", "陳", "楊", "趙", "黃", "周", "吳",
    "徐", "孫", "馬", "胡", "朱", "郭", "何", "羅", "高", "林",
    "鄭", "梁", "謝", "宋", "唐", "許", "韓", "馮", "鄧", "曹",
    "彭", "曾", "蕭", "田", "董", "袁", "潘", "于", "蔣", "蔡",
    "余", "杜", "葉", "程", "魏", "蘇", "盧", "丁", "史", "傅",
]

GIVEN_CHARS: list[str] = [
    "小", "大", "美", "志", "建", "文", "明", "秀", "雅", "偉",
    "芳", "平", "東", "寬", "方", "和", "正", "春", "玲", "珍",
    "英", "傑", "豪", "勝", "福", "貴", "銘", "俊", "淑", "慧",
    "婷", "曉", "宏", "凱", "安", "靜", "嘉", "佳", "翔", "宇",
]

# Pre-build a name pool of 5 000 distinct names for fast random selection.
# Use a seeded RNG so the pool is deterministic; sample() avoids the
# shuffle-then-slice pattern (which was shuffling but discarding the result).
_NAME_POOL_ALL: list[str] = [
    _s + _a + _b
    for _s in SURNAMES
    for _a in GIVEN_CHARS
    for _b in GIVEN_CHARS
]
_rng_seed = random.Random(42)
_NAME_POOL: list[str] = _rng_seed.sample(_NAME_POOL_ALL, 5000)
NAME_POOL_ARR = np.array(_NAME_POOL, dtype=object)

# ── Employee / user / store pools ─────────────────────────────────────────────
PROCESS_USERS: list[str] = [f"EMP-{i:04d}" for i in _rng_seed.sample(range(1000, 9999), 50)]
SYS_USERS: list[str] = [f"EMP-{i:04d}" for i in _rng_seed.sample(range(1000, 9999), 30)]
USR_IDS: list[str] = [f"USR-{i:07d}" for i in range(1_000_000)]
GD_IDS: list[str] = [f"GD-{i:06d}" for i in range(100_000)]

PROCESS_USERS_ARR = np.array(PROCESS_USERS, dtype=object)
SYS_USERS_ARR = np.array(SYS_USERS, dtype=object)
USR_IDS_ARR = np.array(USR_IDS, dtype=object)
GD_IDS_ARR = np.array(GD_IDS, dtype=object)

# ── Date epoch anchors (seconds since Unix epoch) ─────────────────────────────
EPOCH_START = int(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp())
EPOCH_END = int(datetime(2026, 5, 1, tzinfo=timezone.utc).timestamp())
RANGE_SEC = EPOCH_END - EPOCH_START

# ── Distribution weights ───────────────────────────────────────────────────────
STATUS_IDS = [1, 2, 3, 4, 5]
STATUS_W = [0.05, 0.08, 0.07, 0.05, 0.75]

SOURCE_IDS = [1, 2, 3, 4, 5]
SOURCE_W = [0.35, 0.25, 0.20, 0.15, 0.05]

CATSUB_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
CATSUB_W = [0.30, 0.20, 0.15, 0.05, 0.05, 0.10, 0.05, 0.04, 0.03, 0.03]

# Weighted class_id: 1-20, slightly skewed toward lower numbers
CLASS_W = np.array([1 / (i**0.5) for i in range(1, 21)], dtype=float)
CLASS_W /= CLASS_W.sum()

INTCLASS_W = np.array([1 / (i**0.4) for i in range(1, 31)], dtype=float)
INTCLASS_W /= INTCLASS_W.sum()

# ── SQL helpers ────────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS iceberg.raw.raw_tickets (
    prblm_code              VARCHAR,
    prblm_sysdate           TIMESTAMP(6),
    prblm_updateddate       TIMESTAMP(6),
    prblm_donedate          TIMESTAMP(6),
    prblm_preassignenddate  TIMESTAMP(6),
    prblm_preassignbegindate TIMESTAMP(6),
    prblm_status_id         INTEGER,
    prblm_source_id         INTEGER,
    prblm_class_id          INTEGER,
    prblm_intclass_id       INTEGER,
    prblm_perform_id        INTEGER,
    prblm_complain_id       INTEGER,
    prblm_processuser       VARCHAR,
    prblm_doneuser          VARCHAR,
    prblm_sysuser           VARCHAR,
    usr_id                  VARCHAR,
    prblm_name              VARCHAR,
    gd_id                   VARCHAR,
    catsub_id               INTEGER,
    supplier_id             INTEGER,
    prblm_doneatatime       BOOLEAN,
    prblm_forwarddatetime   TIMESTAMP(6),
    prblm_forwardtype       INTEGER,
    prblm_notasreason_id    VARCHAR,
    prblm_notasowndept_id   INTEGER,
    ingested_at             TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    location = 's3://lakehouse/raw/raw_tickets/',
    partitioning = ARRAY['year(prblm_sysdate)']
)
""".strip()

TRUNCATE_SQL = "DELETE FROM iceberg.raw.raw_tickets WHERE 1=1"


# ── Name generation ────────────────────────────────────────────────────────────
def generate_cn_names(n: int, rng: np.random.Generator) -> np.ndarray:
    """Pick n names from the pre-built pool."""
    idxs = rng.integers(0, len(NAME_POOL_ARR), size=n)
    return NAME_POOL_ARR[idxs]


# ── Core batch generation ──────────────────────────────────────────────────────
def generate_batch(
    start_seq: int,
    size: int,
    rng: np.random.Generator,
    year_counters: Counter,
) -> list[dict[str, Any]]:
    """
    Generate `size` ticket records as a list of dicts.
    Returns list in insertion order.
    """
    # --- timestamps ---
    sysdate_sec = EPOCH_START + rng.integers(0, RANGE_SEC, size=size)
    # updateddate: sysdate + 0..30 days
    update_delta = rng.integers(0, 30 * 86400, size=size)
    updateddate_sec = sysdate_sec + update_delta

    # --- status ---
    status_ids = rng.choice(STATUS_IDS, size=size, p=STATUS_W)

    # donedate: NULL for open tickets (status 1-4, ~25% overall)
    is_resolved = status_ids == 5  # 75% resolved
    # also mark some status=4 tickets as resolved-but-not-closed
    donedate_sec = np.where(
        is_resolved,
        updateddate_sec + rng.integers(0, 7200, size=size),  # up to 2h after update
        -1,  # sentinel for NULL
    )

    # preassignenddate: first response within 2-24h of sysdate
    preassign_end_sec = sysdate_sec + rng.integers(7200, 86400, size=size)
    # preassignbegindate: 0..30min before preassignenddate
    preassign_begin_sec = preassign_end_sec - rng.integers(0, 1800, size=size)

    # forwarddatetime: 20% have it, after sysdate
    has_forward = rng.random(size=size) < 0.20
    forward_delta = rng.integers(3600, 7 * 86400, size=size)
    forward_sec = sysdate_sec + forward_delta

    # ingested_at: within 1 hour after sysdate
    ingested_sec = sysdate_sec + rng.integers(0, 3600, size=size)

    # --- categoricals ---
    source_ids = rng.choice(SOURCE_IDS, size=size, p=SOURCE_W)
    class_ids = rng.choice(np.arange(1, 21), size=size, p=CLASS_W)
    intclass_ids = rng.choice(np.arange(1, 31), size=size, p=INTCLASS_W)
    catsub_ids = rng.choice(CATSUB_IDS, size=size, p=CATSUB_W)

    # perform_id: NULL(60%) 1(30%) 2(10%)
    perf_roll = rng.random(size=size)
    perform_ids = np.where(perf_roll < 0.60, -1, np.where(perf_roll < 0.90, 1, 2))

    # complain_id: 1(85%) 2(10%) 3(5%)
    comp_roll = rng.random(size=size)
    complain_ids = np.where(comp_roll < 0.85, 1, np.where(comp_roll < 0.95, 2, 3))

    # prblm_doneatatime: TRUE for 40% of resolved
    done_ata = is_resolved & (rng.random(size=size) < 0.40)

    # forwardtype: NULL unless has_forward
    fwd_type = np.where(has_forward, rng.integers(1, 6, size=size), -1)

    # notasreason_id: NULL(70%) or '1'..'20'
    has_notasreason = rng.random(size=size) < 0.30
    notasreason_raw = rng.integers(1, 21, size=size)

    # notasowndept_id: NULL(70%) or 1-15
    has_notasowndept = rng.random(size=size) < 0.30
    notasowndept_raw = rng.integers(1, 16, size=size)

    # supplier_id: 1-5000
    supplier_ids = rng.integers(1, 5001, size=size)

    # personnel
    processuser_idxs = rng.integers(0, len(PROCESS_USERS_ARR), size=size)
    doneuser_idxs = rng.integers(0, len(PROCESS_USERS_ARR), size=size)
    sysuser_idxs = rng.integers(0, len(SYS_USERS_ARR), size=size)
    usr_idxs = rng.integers(0, len(USR_IDS_ARR), size=size)
    gd_idxs = rng.integers(0, len(GD_IDS_ARR), size=size)

    names = generate_cn_names(size, rng)

    records: list[dict[str, Any]] = []
    for i in range(size):
        ts = datetime.fromtimestamp(int(sysdate_sec[i]), tz=timezone.utc)
        year = ts.year
        year_counters[year] += 1
        seq = year_counters[year]

        prblm_code = f"PRB-{year}-{seq:07d}"

        def ts_str(epoch_sec: int) -> str:
            return datetime.fromtimestamp(int(epoch_sec), tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S.000000"
            )

        record: dict[str, Any] = {
            "prblm_code": prblm_code,
            "prblm_sysdate": ts_str(sysdate_sec[i]),
            "prblm_updateddate": ts_str(updateddate_sec[i]),
            "prblm_donedate": ts_str(donedate_sec[i]) if donedate_sec[i] != -1 else None,
            "prblm_preassignenddate": ts_str(preassign_end_sec[i]),
            "prblm_preassignbegindate": ts_str(preassign_begin_sec[i]),
            "prblm_status_id": int(status_ids[i]),
            "prblm_source_id": int(source_ids[i]),
            "prblm_class_id": int(class_ids[i]),
            "prblm_intclass_id": int(intclass_ids[i]),
            "prblm_perform_id": int(perform_ids[i]) if perform_ids[i] != -1 else None,
            "prblm_complain_id": int(complain_ids[i]),
            "prblm_processuser": str(PROCESS_USERS_ARR[processuser_idxs[i]]),
            "prblm_doneuser": str(PROCESS_USERS_ARR[doneuser_idxs[i]]) if is_resolved[i] else None,
            "prblm_sysuser": str(SYS_USERS_ARR[sysuser_idxs[i]]),
            "usr_id": str(USR_IDS_ARR[usr_idxs[i]]),
            "prblm_name": str(names[i]),
            "gd_id": str(GD_IDS_ARR[gd_idxs[i]]),
            "catsub_id": int(catsub_ids[i]),
            "supplier_id": int(supplier_ids[i]),
            "prblm_doneatatime": bool(done_ata[i]),
            "prblm_forwarddatetime": ts_str(forward_sec[i]) if has_forward[i] else None,
            "prblm_forwardtype": int(fwd_type[i]) if has_forward[i] else None,
            "prblm_notasreason_id": str(notasreason_raw[i]) if has_notasreason[i] else None,
            "prblm_notasowndept_id": int(notasowndept_raw[i]) if has_notasowndept[i] else None,
            "ingested_at": ts_str(ingested_sec[i]),
        }
        records.append(record)

    return records


# ── SQL value formatting ───────────────────────────────────────────────────────
# Compiled pattern that matches exactly the timestamp format we produce:
# "YYYY-MM-DD HH:MM:SS.ffffff"  (26 chars, no timezone suffix)
import re as _re
_TS_RE = _re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}$")


def _sql_val(v: Any) -> str:
    """Format a Python value for safe embedding in a Trino VALUES clause.

    All string values are single-quote-escaped.  Timestamp strings are
    identified by a strict regex (not fragile length/char heuristics) and
    wrapped with TIMESTAMP '…' so Trino parses them correctly.
    """
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        escaped = v.replace("'", "''")
        if _TS_RE.match(v):
            return f"TIMESTAMP '{escaped}'"
        return f"'{escaped}'"
    return f"'{v}'"


COLUMN_ORDER = [
    "prblm_code", "prblm_sysdate", "prblm_updateddate", "prblm_donedate",
    "prblm_preassignenddate", "prblm_preassignbegindate",
    "prblm_status_id", "prblm_source_id", "prblm_class_id", "prblm_intclass_id",
    "prblm_perform_id", "prblm_complain_id",
    "prblm_processuser", "prblm_doneuser", "prblm_sysuser",
    "usr_id", "prblm_name", "gd_id", "catsub_id", "supplier_id",
    "prblm_doneatatime", "prblm_forwarddatetime", "prblm_forwardtype",
    "prblm_notasreason_id", "prblm_notasowndept_id", "ingested_at",
]


def build_insert_sql(records: list[dict[str, Any]]) -> str:
    """Build a single INSERT...VALUES SQL string for a sub-chunk of records."""
    rows: list[str] = []
    for rec in records:
        vals = ", ".join(_sql_val(rec[col]) for col in COLUMN_ORDER)
        rows.append(f"({vals})")
    values_clause = ",\n  ".join(rows)
    return f"INSERT INTO iceberg.raw.raw_tickets VALUES\n  {values_clause}"


# ── S3 cleanup ────────────────────────────────────────────────────────────────
def clean_s3_prefix(endpoint: str, bucket: str, prefix: str, access_key: str, secret_key: str) -> None:
    """Delete all objects under bucket/prefix so CREATE TABLE finds an empty location."""
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
            deleted += 1
    if deleted:
        log.info("S3 cleanup: removed %d object(s) from s3://%s/%s", deleted, bucket, prefix)
    else:
        log.info("S3 cleanup: s3://%s/%s already empty", bucket, prefix)


# ── Connection + retry ─────────────────────────────────────────────────────────
def get_connection(host: str, port: int) -> trino.dbapi.Connection:
    """Return a Trino connection.

    The Trino user is read from the TRINO_USER environment variable and
    defaults to 'admin' when running locally without auth.  No password is
    sent; the local stack uses the no-auth HTTP transport.
    """
    import os
    trino_user = os.environ.get("TRINO_USER", "admin")
    return trino.dbapi.connect(
        host=host,
        port=port,
        user=trino_user,
        catalog="iceberg",
        schema="raw",
        http_scheme="http",
        request_timeout=300,
    )


def execute_with_retry(
    cursor: Any,
    sql: str,
    max_attempts: int = 5,
    base_delay: float = 2.0,
) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            cursor.execute(sql)
            # Trino is async; consume result to confirm completion
            cursor.fetchall()
            return
        except Exception as exc:
            if attempt == max_attempts:
                log.error("Batch failed after %d attempts: %s", max_attempts, exc)
                raise
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
            log.warning(
                "Attempt %d/%d failed (%s). Retrying in %.1fs…",
                attempt, max_attempts, exc, delay,
            )
            time.sleep(delay)


# ── Progress display ───────────────────────────────────────────────────────────
class ProgressTracker:
    def __init__(self, total: int) -> None:
        self.total = total
        self.done = 0
        self.t0 = time.monotonic()
        self._bar = tqdm(total=total, unit="rows", unit_scale=True) if tqdm else None

    def update(self, n: int) -> None:
        self.done += n
        elapsed = time.monotonic() - self.t0
        rate = self.done / elapsed if elapsed > 0 else 0
        pct = 100.0 * self.done / self.total
        if self._bar:
            self._bar.update(n)
            self._bar.set_postfix_str(f"{rate:,.0f} rows/sec")
        else:
            sys.stdout.write(
                f"\rGenerated {self.done:,} / {self.total:,} rows "
                f"({pct:.1f}%) - {rate:,.0f} rows/sec   "
            )
            sys.stdout.flush()

    def close(self) -> None:
        if self._bar:
            self._bar.close()
        else:
            print()
        elapsed = time.monotonic() - self.t0
        log.info(
            "Done: %s rows in %.1fs (avg %.0f rows/sec)",
            f"{self.done:,}", elapsed, self.done / elapsed if elapsed else 0,
        )


# ── Summary statistics ─────────────────────────────────────────────────────────
def print_summary(year_counters: Counter, status_counters: Counter) -> None:
    print("\n" + "=" * 56)
    print("  GENERATION SUMMARY")
    print("=" * 56)
    print("  Rows by year:")
    for yr in sorted(year_counters):
        print(f"    {yr}: {year_counters[yr]:>10,}")
    print("  Rows by status:")
    status_labels = {1: "開啟", 2: "處理中", 3: "待回覆", 4: "已回覆", 5: "結案"}
    for sid in sorted(status_counters):
        label = status_labels.get(sid, str(sid))
        print(f"    {sid} {label}: {status_counters[sid]:>10,}")
    print("=" * 56)


# ── Main ───────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic customer-service ticket data into Iceberg."
    )
    parser.add_argument("--host", default="localhost", help="Trino host (default: localhost)")
    parser.add_argument("--port", type=int, default=8080, help="Trino port (default: 8080)")
    parser.add_argument("--total-rows", type=int, default=10_000_000, help="Total rows to generate")
    parser.add_argument("--batch-size", type=int, default=50_000, help="Generation batch size")
    parser.add_argument(
        "--insert-chunk", type=int, default=1_000,
        help="Rows per single INSERT statement (default: 1000)"
    )
    parser.add_argument("--seed", type=int, default=2024, help="NumPy random seed")
    parser.add_argument("--dry-run", action="store_true", help="Generate data but do not insert")
    parser.add_argument("--truncate", action="store_true", help="Delete all existing rows first")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = np.random.default_rng(args.seed)

    log.info("Ticket generator starting — target: %s rows", f"{args.total_rows:,}")
    log.info("Batch size: %s | Insert chunk: %s | dry-run: %s",
             args.batch_size, args.insert_chunk, args.dry_run)

    conn: trino.dbapi.Connection | None = None
    cursor = None

    if not args.dry_run:
        log.info("Connecting to Trino at %s:%d…", args.host, args.port)
        conn = get_connection(args.host, args.port)
        cursor = conn.cursor()

        s3_endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
        s3_access_key = os.environ.get("MINIO_ROOT_USER", "")
        s3_secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "")

        # Warm up Trino's S3/Netty DNS by probing until we get a non-DNS error
        log.info("Warming up Trino S3 connectivity…")
        for _w in range(60):
            try:
                cursor.execute("SELECT count(*) FROM iceberg.raw.raw_tickets")
                cursor.fetchall()
                break  # table exists and is readable
            except Exception as _we:
                msg = str(_we)
                if "UnknownHostException" in msg or "DNS" in msg.upper():
                    if _w % 6 == 0:
                        log.info("  Trino S3 not reachable yet (attempt %d/60), waiting 5s…", _w + 1)
                    time.sleep(5)
                else:
                    break  # any other error (table not found, etc.) means S3 is reachable
        log.info("Trino S3 connectivity warm-up done.")

        # Check if table already exists — on restart, skip cleanup to preserve data
        cursor.execute("SHOW TABLES IN iceberg.raw LIKE 'raw_tickets'")
        table_exists = bool(cursor.fetchall())

        if table_exists:
            # Validate the table is actually readable (Polaris may have stale
            # registration pointing to deleted S3 metadata after an init-container wipe)
            try:
                cursor.execute("SELECT 1 FROM iceberg.raw.raw_tickets LIMIT 0")
                cursor.fetchall()
                log.info("Table iceberg.raw.raw_tickets already exists — skipping CREATE TABLE.")
            except Exception as probe_exc:
                probe_msg = str(probe_exc)
                if "ICEBERG_CATALOG_ERROR" in probe_msg or "Failed to load table" in probe_msg:
                    log.warning(
                        "Table registered in Polaris but metadata missing in S3 "
                        "(%s). Dropping stale registration and re-creating…", probe_msg
                    )
                    table_exists = False
                    try:
                        cursor.execute("DROP TABLE IF EXISTS iceberg.raw.raw_tickets")
                        cursor.fetchall()
                    except Exception:
                        pass
                else:
                    raise
        else:
            log.info("Table not found — creating (S3 cleaned before each attempt)…")
            for _ct_attempt in range(1, 11):
                if s3_access_key and s3_secret_key:
                    clean_s3_prefix(s3_endpoint, "lakehouse", "raw/raw_tickets/",
                                    s3_access_key, s3_secret_key)
                try:
                    try:
                        cursor.execute("DROP TABLE IF EXISTS iceberg.raw.raw_tickets")
                        cursor.fetchall()
                    except Exception:
                        pass
                    cursor.execute(CREATE_TABLE_SQL)
                    cursor.fetchall()
                    break
                except Exception as exc:
                    if _ct_attempt == 10:
                        log.error("CREATE TABLE failed after 10 attempts: %s", exc)
                        raise
                    delay = min(5.0 * _ct_attempt, 60.0)
                    log.warning(
                        "CREATE TABLE attempt %d/10 failed (%s). Retrying in %.0fs…",
                        _ct_attempt, exc, delay,
                    )
                    time.sleep(delay)

        # Verify the table is actually loadable before starting INSERTs.
        # Polaris may need a moment after CREATE TABLE to fully persist state.
        for _probe in range(1, 31):
            try:
                cursor.execute("SELECT 1 FROM iceberg.raw.raw_tickets LIMIT 0")
                cursor.fetchall()
                break
            except Exception as _pe:
                if _probe == 30:
                    log.error("Table not loadable after 30 probes: %s", _pe)
                    raise
                log.info("  Table not yet loadable (attempt %d/30): %s. Waiting 5s…", _probe, _pe)
                time.sleep(5)
        log.info("Table iceberg.raw.raw_tickets is ready.")

        if args.truncate:
            log.warning("--truncate specified: deleting all existing rows…")
            execute_with_retry(cursor, TRUNCATE_SQL)
            log.info("Truncation complete.")

    year_counters: Counter = Counter()
    status_counters: Counter = Counter()
    total_inserted = 0
    progress = ProgressTracker(args.total_rows)

    batches = math.ceil(args.total_rows / args.batch_size)

    try:
        for batch_idx in range(batches):
            remaining = args.total_rows - total_inserted
            current_size = min(args.batch_size, remaining)
            if current_size <= 0:
                break

            t_gen = time.monotonic()
            records = generate_batch(total_inserted, current_size, rng, year_counters)

            # track status distribution
            for rec in records:
                status_counters[rec["prblm_status_id"]] += 1

            log.debug(
                "Batch %d/%d generated %s rows in %.2fs",
                batch_idx + 1, batches, f"{current_size:,}", time.monotonic() - t_gen,
            )

            if not args.dry_run and cursor is not None:
                # Split batch into insert-chunk-sized pieces to avoid huge SQL strings
                chunks = math.ceil(current_size / args.insert_chunk)
                for chunk_idx in range(chunks):
                    chunk_start = chunk_idx * args.insert_chunk
                    chunk_end = min(chunk_start + args.insert_chunk, current_size)
                    chunk = records[chunk_start:chunk_end]
                    sql = build_insert_sql(chunk)
                    execute_with_retry(cursor, sql)

            total_inserted += current_size
            progress.update(current_size)

    finally:
        # Always release the connection, even if an exception aborts the loop.
        progress.close()
        if conn:
            try:
                conn.close()
            except Exception as close_exc:
                log.warning("Error closing Trino connection: %s", close_exc)

    print_summary(year_counters, status_counters)

    if args.dry_run:
        log.info("Dry-run complete — no rows were inserted.")
    else:
        log.info("All %s rows inserted successfully.", f"{total_inserted:,}")


if __name__ == "__main__":
    main()
