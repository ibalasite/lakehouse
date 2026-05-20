#!/usr/bin/env python3
"""
04_generate_tickets.py
======================
Generates synthetic e-commerce customer-service ticket records and writes
them DIRECTLY to the Iceberg lakehouse via pyiceberg + pyarrow.

This bypasses Trino INSERT entirely — pyiceberg writes Parquet files directly
to MinIO and registers the Iceberg metadata in Polaris.  Typically 100-500x
faster than the SQL INSERT approach.

Usage
-----
    python 04_generate_tickets.py                    # 10M rows (default)
    python 04_generate_tickets.py --rows 200000      # quick test
    python 04_generate_tickets.py --rows 1000000 --batch 100000

Required env vars
-----------------
    MINIO_ROOT_USER       — MinIO access key
    MINIO_ROOT_PASSWORD   — MinIO secret key
    POLARIS_CLIENT_ID     — Polaris OAuth2 client id
    POLARIS_CLIENT_SECRET — Polaris OAuth2 client secret

Optional env vars
-----------------
    S3_ENDPOINT   — e.g. http://minio:9000 (default)
    MINIO_BUCKET  — e.g. lakehouse-local (default)
    POLARIS_HOST  — e.g. polaris (default)
    POLARIS_PORT  — e.g. 8181 (default)
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa

# Polaris: remove the read-delegation header so Polaris uses LOAD_TABLE,
# matching Trino's vended-credentials-enabled=false behaviour.
import pyiceberg.catalog.rest as _pir
_orig_init = _pir.RestCatalog.__init__
def _patched_init(self, *args, **kwargs):
    _orig_init(self, *args, **kwargs)
    self._session.headers.pop("X-Iceberg-Access-Delegation", None)
_pir.RestCatalog.__init__ = _patched_init

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("ticket_gen")


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        log.error("Required environment variable %s is not set.", name)
        sys.exit(1)
    return value


# ── Config ─────────────────────────────────────────────────────────────────────
S3_ENDPOINT    = os.environ.get("S3_ENDPOINT",   "http://minio:9000")
BUCKET         = os.environ.get("MINIO_BUCKET",  "lakehouse-local")
POLARIS_HOST   = os.environ.get("POLARIS_HOST",  "polaris")
POLARIS_PORT   = os.environ.get("POLARIS_PORT",  "8181")
MINIO_USER     = _require_env("MINIO_ROOT_USER")
MINIO_PASS     = _require_env("MINIO_ROOT_PASSWORD")
POLARIS_ID     = _require_env("POLARIS_CLIENT_ID")
POLARIS_SECRET = _require_env("POLARIS_CLIENT_SECRET")

# EDD section 6.2: source data lands in bronze namespace
ICEBERG_NS     = "bronze"
ICEBERG_TABLE  = "raw_tickets"
TABLE_LOCATION = f"s3://{BUCKET}/warehouse/{ICEBERG_NS}/{ICEBERG_TABLE}"

# ── Arrow schema ───────────────────────────────────────────────────────────────
SCHEMA = pa.schema([
    ("prblm_code",               pa.string()),
    ("prblm_sysdate",            pa.timestamp("us", tz="UTC")),
    ("prblm_updateddate",        pa.timestamp("us", tz="UTC")),
    ("prblm_donedate",           pa.timestamp("us", tz="UTC")),
    ("prblm_preassignenddate",   pa.timestamp("us", tz="UTC")),
    ("prblm_preassignbegindate", pa.timestamp("us", tz="UTC")),
    ("prblm_status_id",          pa.int32()),
    ("prblm_source_id",          pa.int32()),
    ("prblm_class_id",           pa.int32()),
    ("prblm_intclass_id",        pa.int32()),
    ("prblm_perform_id",         pa.int32()),
    ("prblm_complain_id",        pa.int32()),
    ("prblm_processuser",        pa.string()),
    ("prblm_doneuser",           pa.string()),
    ("prblm_sysuser",            pa.string()),
    ("usr_id",                   pa.string()),
    ("prblm_name",               pa.string()),
    ("gd_id",                    pa.string()),
    ("catsub_id",                pa.int32()),
    ("supplier_id",              pa.int32()),
    ("prblm_doneatatime",        pa.bool_()),
    ("prblm_forwarddatetime",    pa.timestamp("us", tz="UTC")),
    ("prblm_forwardtype",        pa.int32()),
    ("prblm_notasreason_id",     pa.string()),
    ("prblm_notasowndept_id",    pa.int32()),
    ("ingested_at",              pa.timestamp("us", tz="UTC")),
])

# ── Name / ID pools ────────────────────────────────────────────────────────────
SURNAMES = ["王","李","張","劉","陳","楊","趙","黃","周","吳",
            "徐","孫","馬","胡","朱","郭","何","羅","高","林",
            "鄭","梁","謝","宋","唐","許","韓","馮","鄧","曹",
            "彭","曾","蕭","田","董","袁","潘","于","蔣","蔡",
            "余","杜","葉","程","魏","蘇","盧","丁","史","傅"]
GIVEN_CHARS = ["小","大","美","志","建","文","明","秀","雅","偉",
               "芳","平","東","寬","方","和","正","春","玲","珍",
               "英","傑","豪","勝","福","貴","銘","俊","淑","慧",
               "婷","曉","宏","凱","安","靜","嘉","佳","翔","宇"]

_seed_rng = random.Random(42)
_NAME_POOL_ALL = [s+a+b for s in SURNAMES for a in GIVEN_CHARS for b in GIVEN_CHARS]
NAME_POOL     = np.array(_seed_rng.sample(_NAME_POOL_ALL, 5000), dtype=object)
PROCESS_USERS = np.array([f"EMP-{i:04d}" for i in _seed_rng.sample(range(1000,9999),50)], dtype=object)
SYS_USERS     = np.array([f"EMP-{i:04d}" for i in _seed_rng.sample(range(1000,9999),30)], dtype=object)
USR_IDS       = np.array([f"USR-{i:07d}" for i in range(1_000_000)], dtype=object)
GD_IDS        = np.array([f"GD-{i:06d}"  for i in range(100_000)], dtype=object)

EPOCH_START = int(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp())
EPOCH_END   = int(datetime(2026, 5, 1, tzinfo=timezone.utc).timestamp())
RANGE_SEC   = EPOCH_END - EPOCH_START

STATUS_W  = [0.05, 0.08, 0.07, 0.05, 0.75]
SOURCE_W  = [0.35, 0.25, 0.20, 0.15, 0.05]
CATSUB_W  = [0.30, 0.20, 0.15, 0.05, 0.05, 0.10, 0.05, 0.04, 0.03, 0.03]
CLASS_W   = np.array([1/(i**0.5) for i in range(1,21)], dtype=float); CLASS_W /= CLASS_W.sum()
INTCLS_W  = np.array([1/(i**0.4) for i in range(1,31)], dtype=float); INTCLS_W /= INTCLS_W.sum()


# ── Batch generation ───────────────────────────────────────────────────────────

def _sec_to_us(arr: np.ndarray) -> pa.Array:
    """Convert seconds-since-epoch int64 array to pyarrow timestamp[us, UTC]."""
    return pa.array((arr * 1_000_000).astype("int64"), type=pa.timestamp("us", tz="UTC"))


def generate_batch(size: int, batch_offset: int, rng: np.random.Generator) -> pa.Table:
    sysdate_s   = EPOCH_START + rng.integers(0, RANGE_SEC, size=size)
    update_s    = sysdate_s + rng.integers(0, 30*86400, size=size)

    status_ids  = rng.choice([1,2,3,4,5], size=size, p=STATUS_W)
    is_resolved = status_ids == 5
    done_s      = np.where(is_resolved, update_s + rng.integers(0,7200,size=size), -1)

    preend_s    = sysdate_s + rng.integers(7200, 86400, size=size)
    prebegin_s  = preend_s  - rng.integers(0, 1800, size=size)

    has_fwd     = rng.random(size=size) < 0.20
    fwd_s       = sysdate_s + rng.integers(3600, 7*86400, size=size)

    ingested_s  = sysdate_s + rng.integers(0, 3600, size=size)

    source_ids  = rng.choice([1,2,3,4,5], size=size, p=SOURCE_W)
    class_ids   = rng.choice(np.arange(1,21), size=size, p=CLASS_W)
    intcls_ids  = rng.choice(np.arange(1,31), size=size, p=INTCLS_W)
    catsub_ids  = rng.choice([1,2,3,4,5,6,7,8,9,10], size=size, p=CATSUB_W)

    perf_roll   = rng.random(size=size)
    perf_ids    = np.where(perf_roll<0.60, None, np.where(perf_roll<0.90, 1, 2))
    perf_arr    = pa.array([None if p is None else int(p) for p in perf_ids], type=pa.int32())

    comp_roll   = rng.random(size=size)
    comp_ids    = np.where(comp_roll<0.85, 1, np.where(comp_roll<0.95, 2, 3))

    done_ata    = is_resolved & (rng.random(size=size) < 0.40)
    fwd_type    = np.where(has_fwd, rng.integers(1,6,size=size), -1)

    has_notas   = rng.random(size=size) < 0.30
    notas_raw   = rng.integers(1,21,size=size)
    has_nodept  = rng.random(size=size) < 0.30
    nodept_raw  = rng.integers(1,16,size=size)

    supplier_ids = rng.integers(1,5001,size=size)

    proc_idx    = rng.integers(0, len(PROCESS_USERS), size=size)
    done_idx    = rng.integers(0, len(PROCESS_USERS), size=size)
    sys_idx     = rng.integers(0, len(SYS_USERS), size=size)
    usr_idx     = rng.integers(0, len(USR_IDS), size=size)
    gd_idx      = rng.integers(0, len(GD_IDS), size=size)
    name_idx    = rng.integers(0, len(NAME_POOL), size=size)

    # build prblm_code — simple sequential per-batch
    codes = np.array([
        f"PRB-{datetime.fromtimestamp(int(sysdate_s[i]),tz=timezone.utc).year}-{batch_offset+i+1:07d}"
        for i in range(size)
    ], dtype=object)

    def nullable_ts(s_arr, mask):
        vals = np.where(mask, s_arr * 1_000_000, pa.NA)
        return pa.array([int(v) if v is not pa.NA and v != -1*1_000_000 else None for v in vals],
                        type=pa.int64())

    done_us     = pa.array([int(done_s[i]*1_000_000) if done_s[i]!=-1 else None for i in range(size)],
                           type=pa.int64())
    fwd_us      = pa.array([int(fwd_s[i]*1_000_000)  if has_fwd[i]  else None for i in range(size)],
                           type=pa.int64())

    def cast_ts(arr):
        return pa.array(arr.tolist(), type=pa.timestamp("us", tz="UTC"))

    return pa.table({
        "prblm_code":               pa.array(codes.tolist()),
        "prblm_sysdate":            _sec_to_us(sysdate_s),
        "prblm_updateddate":        _sec_to_us(update_s),
        "prblm_donedate":           pa.array(
                                        [int(done_s[i]*1_000_000) if done_s[i]!=-1 else None for i in range(size)],
                                        type=pa.timestamp("us", tz="UTC")),
        "prblm_preassignenddate":   _sec_to_us(preend_s),
        "prblm_preassignbegindate": _sec_to_us(prebegin_s),
        "prblm_status_id":          pa.array(status_ids.astype("int32").tolist(), type=pa.int32()),
        "prblm_source_id":          pa.array(source_ids.astype("int32").tolist(), type=pa.int32()),
        "prblm_class_id":           pa.array(class_ids.astype("int32").tolist(),  type=pa.int32()),
        "prblm_intclass_id":        pa.array(intcls_ids.astype("int32").tolist(), type=pa.int32()),
        "prblm_perform_id":         perf_arr,
        "prblm_complain_id":        pa.array(comp_ids.astype("int32").tolist(),   type=pa.int32()),
        "prblm_processuser":        pa.array(PROCESS_USERS[proc_idx].tolist()),
        "prblm_doneuser":           pa.array(
                                        [str(PROCESS_USERS[done_idx[i]]) if is_resolved[i] else None for i in range(size)]),
        "prblm_sysuser":            pa.array(SYS_USERS[sys_idx].tolist()),
        "usr_id":                   pa.array(USR_IDS[usr_idx].tolist()),
        "prblm_name":               pa.array(NAME_POOL[name_idx].tolist()),
        "gd_id":                    pa.array(GD_IDS[gd_idx].tolist()),
        "catsub_id":                pa.array(catsub_ids.astype("int32").tolist(),    type=pa.int32()),
        "supplier_id":              pa.array(supplier_ids.astype("int32").tolist(),  type=pa.int32()),
        "prblm_doneatatime":        pa.array(done_ata.tolist()),
        "prblm_forwarddatetime":    pa.array(
                                        [int(fwd_s[i]*1_000_000) if has_fwd[i] else None for i in range(size)],
                                        type=pa.timestamp("us", tz="UTC")),
        "prblm_forwardtype":        pa.array(
                                        [int(fwd_type[i]) if has_fwd[i] else None for i in range(size)],
                                        type=pa.int32()),
        "prblm_notasreason_id":     pa.array(
                                        [str(notas_raw[i]) if has_notas[i] else None for i in range(size)]),
        "prblm_notasowndept_id":    pa.array(
                                        [int(nodept_raw[i]) if has_nodept[i] else None for i in range(size)],
                                        type=pa.int32()),
        "ingested_at":              _sec_to_us(ingested_s),
    }, schema=SCHEMA)


# ── Catalog ────────────────────────────────────────────────────────────────────

def get_catalog() -> RestCatalog:
    return RestCatalog(
        name="polaris",
        uri=f"http://{POLARIS_HOST}:{POLARIS_PORT}/api/catalog",
        warehouse="lakehouse",
        credential=f"{POLARIS_ID}:{POLARIS_SECRET}",
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "py-io-impl":             "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint":             S3_ENDPOINT,
            "s3.access-key-id":        MINIO_USER,
            "s3.secret-access-key":    MINIO_PASS,
            "s3.path-style-access":    "true",
        },
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bulk-generate synthetic tickets via pyiceberg")
    p.add_argument("--rows",  type=int, default=10_000_000, help="Total rows (default: 10M)")
    p.add_argument("--batch", type=int, default=200_000,    help="Rows per append (default: 200k)")
    p.add_argument("--seed",  type=int, default=2024)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    rng = np.random.default_rng(args.seed)

    log.info("Bulk ticket generator — %s rows, %s per batch, pyiceberg direct write",
             f"{args.rows:,}", f"{args.batch:,}")
    log.info("Target: iceberg.%s.%s @ %s", ICEBERG_NS, ICEBERG_TABLE, TABLE_LOCATION)

    catalog = get_catalog()
    identifier = (ICEBERG_NS, ICEBERG_TABLE)

    # Get or create the Iceberg table
    try:
        iceberg_table = catalog.load_table(identifier)
        log.info("Loaded existing table %s.%s", *identifier)
    except NoSuchTableError:
        log.info("Creating table %s.%s ...", *identifier)
        iceberg_table = catalog.create_table(
            identifier=identifier,
            schema=SCHEMA,
            location=TABLE_LOCATION,
            partition_spec=PartitionSpec(
                PartitionField(
                    source_id=SCHEMA.get_field_index("prblm_sysdate") + 1,
                    field_id=1000,
                    transform=YearTransform(),
                    name="prblm_sysdate_year",
                )
            ),
        )
        log.info("Table created.")

    total_written = 0
    t0 = time.monotonic()
    batches = (args.rows + args.batch - 1) // args.batch

    for i in range(batches):
        size = min(args.batch, args.rows - total_written)
        if size <= 0:
            break
        t_batch = time.monotonic()
        batch = generate_batch(size, total_written, rng)
        iceberg_table.append(batch)
        total_written += size
        elapsed = time.monotonic() - t0
        rate = total_written / elapsed if elapsed > 0 else 0
        log.info(
            "Batch %d/%d — %s rows appended in %.1fs  |  total %s  (%.0f rows/sec)",
            i+1, batches, f"{size:,}", time.monotonic()-t_batch,
            f"{total_written:,}", rate,
        )

    log.info("Done: %s rows in %.1fs (avg %.0f rows/sec)",
             f"{total_written:,}", time.monotonic()-t0,
             total_written/(time.monotonic()-t0) if time.monotonic()-t0 > 0 else 0)


if __name__ == "__main__":
    main()
