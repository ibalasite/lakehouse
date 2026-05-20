#!/usr/bin/env python3
"""
bulk_load_parquet.py
====================
Fast bulk loader: generates synthetic ticket data as Parquet files,
uploads directly to MinIO, then registers the table via Trino.

This bypasses Trino INSERT VALUES entirely — typically 100-500x faster.

Usage
-----
    python3 bulk_load_parquet.py                      # 1M rows (default)
    python3 bulk_load_parquet.py --rows 100000        # quick test
    python3 bulk_load_parquet.py --rows 5000000       # 5M rows

Speed: ~500k-1M rows/sec on local hardware.
"""

from __future__ import annotations

import argparse
import io
import logging
import math
import os
import random
import time
from collections import Counter
from datetime import datetime, timezone

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import trino.dbapi

# Polaris uses LOAD_TABLE_WITH_READ_DELEGATION when X-Iceberg-Access-Delegation
# header is present. Remove it so Polaris uses LOAD_TABLE instead — matching
# Trino's vended-credentials-enabled=false behaviour.
import pyiceberg.catalog.rest as _pir
_orig_init = _pir.RestCatalog.__init__
def _patched_init(self, *args, **kwargs):
    _orig_init(self, *args, **kwargs)
    self._session.headers.pop("X-Iceberg-Access-Delegation", None)
_pir.RestCatalog.__init__ = _patched_init

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("bulk_load")


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        log.error("Required environment variable %s is not set. Run: source .env", name)
        sys.exit(1)
    return value


# ── Config ─────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
MINIO_USER     = _require_env("MINIO_ROOT_USER")
MINIO_PASS     = _require_env("MINIO_ROOT_PASSWORD")
BUCKET         = "lakehouse"
S3_PREFIX      = "raw/raw_tickets"

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
POLARIS_HOST = os.environ.get("POLARIS_HOST", "localhost")
POLARIS_PORT = os.environ.get("POLARIS_PORT", "8181")
POLARIS_CLIENT_ID     = _require_env("POLARIS_CLIENT_ID")
POLARIS_CLIENT_SECRET = _require_env("POLARIS_CLIENT_SECRET")

# ── Name pools (same as original script) ──────────────────────────────────────
SURNAMES = ["王","李","張","劉","陳","楊","趙","黃","周","吳",
            "徐","孫","馬","胡","朱","郭","何","羅","高","林",
            "鄭","梁","謝","宋","唐","許","韓","馮","鄧","曹",
            "彭","曾","蕭","田","董","袁","潘","于","蔣","蔡",
            "余","杜","葉","程","魏","蘇","盧","丁","史","傅"]
GIVEN_CHARS = ["小","大","美","志","建","文","明","秀","雅","偉",
               "芳","平","東","寬","方","和","正","春","玲","珍",
               "英","傑","豪","勝","福","貴","銘","俊","淑","慧",
               "婷","曉","宏","凱","安","靜","嘉","佳","翔","宇"]

_rng_seed = random.Random(42)
_NAME_POOL_ALL = [s+a+b for s in SURNAMES for a in GIVEN_CHARS for b in GIVEN_CHARS]
NAME_POOL_ARR  = np.array(_rng_seed.sample(_NAME_POOL_ALL, 5000), dtype=object)

PROCESS_USERS_ARR = np.array([f"EMP-{i:04d}" for i in _rng_seed.sample(range(1000,9999),50)], dtype=object)
SYS_USERS_ARR     = np.array([f"EMP-{i:04d}" for i in _rng_seed.sample(range(1000,9999),30)], dtype=object)
USR_IDS_ARR       = np.array([f"USR-{i:07d}" for i in range(1_000_000)], dtype=object)
GD_IDS_ARR        = np.array([f"GD-{i:06d}"  for i in range(100_000)], dtype=object)

EPOCH_START = int(datetime(2022,1,1,tzinfo=timezone.utc).timestamp())
EPOCH_END   = int(datetime(2026,5,1,tzinfo=timezone.utc).timestamp())
RANGE_SEC   = EPOCH_END - EPOCH_START

STATUS_IDS = [1,2,3,4,5];  STATUS_W = [0.05,0.08,0.07,0.05,0.75]
SOURCE_IDS = [1,2,3,4,5];  SOURCE_W = [0.35,0.25,0.20,0.15,0.05]
CATSUB_IDS = list(range(1,11))
CATSUB_W   = [0.30,0.20,0.15,0.05,0.05,0.10,0.05,0.04,0.03,0.03]
CLASS_W    = np.array([1/(i**0.5) for i in range(1,21)], dtype=float); CLASS_W /= CLASS_W.sum()
INTCLASS_W = np.array([1/(i**0.4) for i in range(1,31)], dtype=float); INTCLASS_W /= INTCLASS_W.sum()

# ── Arrow schema ───────────────────────────────────────────────────────────────
SCHEMA = pa.schema([
    ("prblm_code",              pa.string()),
    ("prblm_sysdate",           pa.timestamp("us", tz="UTC")),
    ("prblm_updateddate",       pa.timestamp("us", tz="UTC")),
    ("prblm_donedate",          pa.timestamp("us", tz="UTC")),
    ("prblm_preassignenddate",  pa.timestamp("us", tz="UTC")),
    ("prblm_preassignbegindate",pa.timestamp("us", tz="UTC")),
    ("prblm_status_id",         pa.int32()),
    ("prblm_source_id",         pa.int32()),
    ("prblm_class_id",          pa.int32()),
    ("prblm_intclass_id",       pa.int32()),
    ("prblm_perform_id",        pa.int32()),
    ("prblm_complain_id",       pa.int32()),
    ("prblm_processuser",       pa.string()),
    ("prblm_doneuser",          pa.string()),
    ("prblm_sysuser",           pa.string()),
    ("usr_id",                  pa.string()),
    ("prblm_name",              pa.string()),
    ("gd_id",                   pa.string()),
    ("catsub_id",               pa.int32()),
    ("supplier_id",             pa.int32()),
    ("prblm_doneatatime",       pa.bool_()),
    ("prblm_forwarddatetime",   pa.timestamp("us", tz="UTC")),
    ("prblm_forwardtype",       pa.int32()),
    ("prblm_notasreason_id",    pa.string()),
    ("prblm_notasowndept_id",   pa.int32()),
    ("ingested_at",             pa.timestamp("us", tz="UTC")),
])


def _sec_to_us(arr: np.ndarray) -> np.ndarray:
    return arr.astype(np.int64) * 1_000_000


def generate_arrow_batch(size: int, rng: np.random.Generator, year_counters: Counter) -> pa.Table:
    sysdate_sec     = EPOCH_START + rng.integers(0, RANGE_SEC, size=size)
    update_delta    = rng.integers(0, 30*86400, size=size)
    updateddate_sec = sysdate_sec + update_delta

    status_ids  = rng.choice(STATUS_IDS, size=size, p=STATUS_W)
    is_resolved = status_ids == 5

    donedate_us = np.where(
        is_resolved,
        _sec_to_us(updateddate_sec + rng.integers(0, 7200, size=size)),
        np.int64(-1),
    )

    preassign_end_sec   = sysdate_sec + rng.integers(7200, 86400, size=size)
    preassign_begin_sec = preassign_end_sec - rng.integers(0, 1800, size=size)

    has_forward  = rng.random(size=size) < 0.20
    forward_sec  = sysdate_sec + rng.integers(3600, 7*86400, size=size)
    ingested_sec = sysdate_sec + rng.integers(0, 3600, size=size)

    source_ids   = rng.choice(SOURCE_IDS, size=size, p=SOURCE_W)
    class_ids    = rng.choice(np.arange(1,21), size=size, p=CLASS_W).astype(np.int32)
    intclass_ids = rng.choice(np.arange(1,31), size=size, p=INTCLASS_W).astype(np.int32)
    catsub_ids   = rng.choice(CATSUB_IDS, size=size, p=CATSUB_W)

    perf_roll    = rng.random(size=size)
    perform_ids  = np.where(perf_roll < 0.60, -1, np.where(perf_roll < 0.90, 1, 2)).astype(np.int32)

    comp_roll    = rng.random(size=size)
    complain_ids = np.where(comp_roll < 0.85, 1, np.where(comp_roll < 0.95, 2, 3)).astype(np.int32)

    done_ata     = is_resolved & (rng.random(size=size) < 0.40)
    fwd_type     = np.where(has_forward, rng.integers(1, 6, size=size), -1).astype(np.int32)

    has_notasreason  = rng.random(size=size) < 0.30
    notasreason_raw  = rng.integers(1, 21, size=size)
    has_notasowndept = rng.random(size=size) < 0.30
    notasowndept_raw = rng.integers(1, 16, size=size).astype(np.int32)

    supplier_ids = rng.integers(1, 5001, size=size).astype(np.int32)

    names = NAME_POOL_ARR[rng.integers(0, len(NAME_POOL_ARR), size=size)]
    processusers = PROCESS_USERS_ARR[rng.integers(0, len(PROCESS_USERS_ARR), size=size)]
    doneusers    = PROCESS_USERS_ARR[rng.integers(0, len(PROCESS_USERS_ARR), size=size)]
    sysusers     = SYS_USERS_ARR[rng.integers(0, len(SYS_USERS_ARR), size=size)]
    usr_ids      = USR_IDS_ARR[rng.integers(0, len(USR_IDS_ARR), size=size)]
    gd_ids       = GD_IDS_ARR[rng.integers(0, len(GD_IDS_ARR), size=size)]

    # Build prblm_code using per-year counters
    codes = []
    for i in range(size):
        ts   = datetime.fromtimestamp(int(sysdate_sec[i]), tz=timezone.utc)
        year = ts.year
        year_counters[year] += 1
        codes.append(f"PRB-{year}-{year_counters[year]:07d}")

    def nullable_ts(us_arr: np.ndarray, mask: np.ndarray) -> pa.Array:
        return pa.array(
            [int(v) if m else None for v, m in zip(us_arr.tolist(), mask.tolist())],
            type=pa.timestamp("us", tz="UTC"),
        )

    def nullable_int32(arr: np.ndarray, mask: np.ndarray) -> pa.Array:
        return pa.array(
            [int(v) if m else None for v, m in zip(arr.tolist(), mask.tolist())],
            type=pa.int32(),
        )

    def nullable_str(arr: np.ndarray, mask: np.ndarray) -> pa.Array:
        return pa.array(
            [str(v) if m else None for v, m in zip(arr.tolist(), mask.tolist())],
            type=pa.string(),
        )

    table = pa.table({
        "prblm_code":               pa.array(codes, type=pa.string()),
        "prblm_sysdate":            pa.array(_sec_to_us(sysdate_sec).tolist(), type=pa.timestamp("us", tz="UTC")),
        "prblm_updateddate":        pa.array(_sec_to_us(updateddate_sec).tolist(), type=pa.timestamp("us", tz="UTC")),
        "prblm_donedate":           nullable_ts(_sec_to_us(updateddate_sec + rng.integers(0,7200,size=size)), is_resolved),
        "prblm_preassignenddate":   pa.array(_sec_to_us(preassign_end_sec).tolist(), type=pa.timestamp("us", tz="UTC")),
        "prblm_preassignbegindate": pa.array(_sec_to_us(preassign_begin_sec).tolist(), type=pa.timestamp("us", tz="UTC")),
        "prblm_status_id":          pa.array(status_ids.astype(np.int32).tolist(), type=pa.int32()),
        "prblm_source_id":          pa.array(np.array(source_ids, dtype=np.int32).tolist(), type=pa.int32()),
        "prblm_class_id":           pa.array(class_ids.tolist(), type=pa.int32()),
        "prblm_intclass_id":        pa.array(intclass_ids.tolist(), type=pa.int32()),
        "prblm_perform_id":         nullable_int32(perform_ids, perform_ids != -1),
        "prblm_complain_id":        pa.array(complain_ids.tolist(), type=pa.int32()),
        "prblm_processuser":        pa.array(processusers.tolist(), type=pa.string()),
        "prblm_doneuser":           nullable_str(doneusers, is_resolved),
        "prblm_sysuser":            pa.array(sysusers.tolist(), type=pa.string()),
        "usr_id":                   pa.array(usr_ids.tolist(), type=pa.string()),
        "prblm_name":               pa.array(names.tolist(), type=pa.string()),
        "gd_id":                    pa.array(gd_ids.tolist(), type=pa.string()),
        "catsub_id":                pa.array(np.array(catsub_ids, dtype=np.int32).tolist(), type=pa.int32()),
        "supplier_id":              pa.array(supplier_ids.tolist(), type=pa.int32()),
        "prblm_doneatatime":        pa.array(done_ata.tolist(), type=pa.bool_()),
        "prblm_forwarddatetime":    nullable_ts(_sec_to_us(forward_sec), has_forward),
        "prblm_forwardtype":        nullable_int32(fwd_type, has_forward),
        "prblm_notasreason_id":     nullable_str(notasreason_raw.astype(str), has_notasreason),
        "prblm_notasowndept_id":    nullable_int32(notasowndept_raw, has_notasowndept),
        "ingested_at":              pa.array(_sec_to_us(ingested_sec).tolist(), type=pa.timestamp("us", tz="UTC")),
    }, schema=SCHEMA)

    return table


def get_iceberg_catalog() -> any:
    from pyiceberg.catalog.rest import RestCatalog
    return RestCatalog(
        name="polaris",
        uri=f"http://{POLARIS_HOST}:{POLARIS_PORT}/api/catalog",
        warehouse=BUCKET,
        credential=f"{POLARIS_CLIENT_ID}:{POLARIS_CLIENT_SECRET}",
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_USER,
            "s3.secret-access-key": MINIO_PASS,
            "s3.path-style-access": "true",
        }
    )


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS iceberg.raw.raw_tickets (
    prblm_code              VARCHAR,
    prblm_sysdate           TIMESTAMP(6) WITH TIME ZONE,
    prblm_updateddate       TIMESTAMP(6) WITH TIME ZONE,
    prblm_donedate          TIMESTAMP(6) WITH TIME ZONE,
    prblm_preassignenddate  TIMESTAMP(6) WITH TIME ZONE,
    prblm_preassignbegindate TIMESTAMP(6) WITH TIME ZONE,
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
    prblm_forwarddatetime   TIMESTAMP(6) WITH TIME ZONE,
    prblm_forwardtype       INTEGER,
    prblm_notasreason_id    VARCHAR,
    prblm_notasowndept_id   INTEGER,
    ingested_at             TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    location = 's3://lakehouse/raw/raw_tickets/',
    partitioning = ARRAY['year(prblm_sysdate)']
)
""".strip()


def setup_trino_table(host: str, port: int) -> None:
    log.info("Connecting to Trino at %s:%d…", host, port)
    conn = trino.dbapi.connect(host=host, port=port, user="admin",
                               catalog="iceberg", schema="raw",
                               http_scheme="http", request_timeout=120)
    cur = conn.cursor()

    # Drop stale Polaris registration if needed
    try:
        cur.execute("SELECT count(*) FROM iceberg.raw.raw_tickets")
        cur.fetchall()
        log.info("Table already exists and is readable — will append.")
        conn.close()
        return
    except Exception as e:
        msg = str(e)
        if "Failed to load" in msg or "ICEBERG_CATALOG_ERROR" in msg or "does not exist" in msg.lower():
            log.info("Stale or missing table — recreating…")
            try:
                cur.execute("DROP TABLE IF EXISTS iceberg.raw.raw_tickets")
                cur.fetchall()
            except Exception:
                pass
        else:
            log.warning("Table probe: %s", e)

    cur.execute(CREATE_TABLE_SQL)
    cur.fetchall()
    log.info("Table iceberg.raw.raw_tickets created.")
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fast Parquet bulk loader for Iceberg lakehouse")
    parser.add_argument("--rows",       type=int, default=1_000_000, help="Total rows (default: 1M)")
    parser.add_argument("--batch-size", type=int, default=200_000,   help="Rows per Parquet file (default: 200k)")
    parser.add_argument("--seed",       type=int, default=2024)
    parser.add_argument("--skip-trino", action="store_true", help="Skip Trino table setup (files only)")
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)

    # Setup: drop stale Polaris table registration then recreate via Trino
    if not args.skip_trino:
        setup_trino_table(TRINO_HOST, TRINO_PORT)

    # Open pyiceberg table for direct Iceberg writes (bypasses Trino INSERT)
    iceberg_catalog = get_iceberg_catalog()
    iceberg_table = iceberg_catalog.load_table("raw.raw_tickets")
    log.info("Iceberg table loaded: %s", iceberg_table.location())

    total_rows  = args.rows
    batch_size  = args.batch_size
    batches     = math.ceil(total_rows / batch_size)
    year_counters: Counter = Counter()
    total_bytes = 0
    t0 = time.monotonic()

    log.info("Generating %s rows in %d batches of %s via pyiceberg…",
             f"{total_rows:,}", batches, f"{batch_size:,}")

    for i in range(batches):
        size = min(batch_size, total_rows - i * batch_size)
        if size <= 0:
            break

        arrow_table = generate_arrow_batch(size, rng, year_counters)
        iceberg_table.append(arrow_table)
        total_bytes += arrow_table.nbytes

        elapsed = time.monotonic() - t0
        done    = min((i+1)*batch_size, total_rows)
        rate    = done / elapsed
        log.info("Batch %d/%d: %s rows appended (%.0f rows/sec, %.1f MB in-mem)",
                 i+1, batches, f"{size:,}", rate, arrow_table.nbytes/1_024/1_024)

    elapsed = time.monotonic() - t0
    log.info("Done: %s rows in %.1fs (%.0f rows/sec)",
             f"{total_rows:,}", elapsed, total_rows/elapsed)

    if not args.skip_trino:
        log.info("Verifying via Trino…")
        conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="admin",
                                   catalog="iceberg", schema="raw",
                                   http_scheme="http", request_timeout=120)
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM iceberg.raw.raw_tickets")
        count = cur.fetchone()[0]
        conn.close()
        log.info("Trino count: %s rows", f"{count:,}")


if __name__ == "__main__":
    main()
