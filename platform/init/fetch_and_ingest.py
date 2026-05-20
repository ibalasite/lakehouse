#!/usr/bin/env python3
"""
fetch_and_ingest.py
===================
Fetches synthetic ticket rows from the data-source pod and appends them to
the raw_tickets Iceberg table via pyiceberg.

Usage (inside the Airflow scheduler pod):
    python3 /opt/airflow/scripts/fetch_and_ingest.py

Required env vars (all supplied by Airflow scheduler's secretKeyRef):
    MINIO_ROOT_USER       — MinIO access key
    MINIO_ROOT_PASSWORD   — MinIO secret key
    POLARIS_CLIENT_ID     — Polaris OAuth2 client id
    POLARIS_CLIENT_SECRET — Polaris OAuth2 client secret

Optional env vars (plain values, set by airflow.yaml):
    DATA_SOURCE_URL       — e.g. http://datasource:8080 (default)
    S3_ENDPOINT           — e.g. http://minio:9000 (default)
    POLARIS_HOST          — e.g. polaris (default)
    POLARIS_PORT          — e.g. 8181 (default)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import urllib.request
from datetime import datetime, timezone

import pyarrow as pa

# Polaris uses LOAD_TABLE_WITH_READ_DELEGATION when X-Iceberg-Access-Delegation
# header is present. Remove it so Polaris uses LOAD_TABLE — matching Trino's
# vended-credentials-enabled=false behaviour.
import pyiceberg.catalog.rest as _pir

_orig_init = _pir.RestCatalog.__init__


def _patched_init(self, *args, **kwargs):
    _orig_init(self, *args, **kwargs)
    self._session.headers.pop("X-Iceberg-Access-Delegation", None)


_pir.RestCatalog.__init__ = _patched_init

from pyiceberg.catalog.rest import RestCatalog  # noqa: E402 — after patch

logging.basicConfig(
    level=logging.INFO,
    format="[fetch-ingest] %(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fetch_ingest")


# ── Configuration ─────────────────────────────────────────────────────────────

def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        log.error("Required environment variable %s is not set.", name)
        sys.exit(1)
    return value


DATA_SOURCE_URL    = os.environ.get("DATA_SOURCE_URL",  "http://datasource:8080")
S3_ENDPOINT        = os.environ.get("S3_ENDPOINT",      "http://minio:9000")
POLARIS_HOST       = os.environ.get("POLARIS_HOST",     "polaris")
POLARIS_PORT       = os.environ.get("POLARIS_PORT",     "8181")
MINIO_USER         = _require_env("MINIO_ROOT_USER")
MINIO_PASS         = _require_env("MINIO_ROOT_PASSWORD")
POLARIS_ID         = _require_env("POLARIS_CLIENT_ID")
POLARIS_SECRET     = _require_env("POLARIS_CLIENT_SECRET")

# EDD section 6.2: bucket is lakehouse-local; source data goes to bronze namespace
BUCKET             = os.environ.get("MINIO_BUCKET", "lakehouse-local")
ICEBERG_TABLE      = ("bronze", "raw_tickets")


# ── Arrow schema — must match the Iceberg table definition exactly ────────────

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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_ts(s: str | None) -> int | None:
    """Parse ISO-8601 string → microseconds-since-epoch (or None)."""
    if s is None:
        return None
    s = s.rstrip("Z").replace("+00:00", "")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: {s!r}")


def _col(rows: list[dict], key: str, dtype: pa.DataType) -> pa.Array:
    values = [r.get(key) for r in rows]
    if pa.types.is_timestamp(dtype):
        us_vals = [_parse_ts(v) for v in values]
        return pa.array(us_vals, type=dtype)
    if pa.types.is_integer(dtype):
        return pa.array([int(v) if v is not None else None for v in values], type=dtype)
    if pa.types.is_boolean(dtype):
        return pa.array([bool(v) if v is not None else None for v in values], type=dtype)
    return pa.array(values, type=dtype)


def rows_to_table(rows: list[dict]) -> pa.Table:
    arrays = {field.name: _col(rows, field.name, field.type) for field in SCHEMA}
    return pa.table(arrays, schema=SCHEMA)


# ── Iceberg catalog ───────────────────────────────────────────────────────────

def get_catalog() -> RestCatalog:
    return RestCatalog(
        name="polaris",
        uri=f"http://{POLARIS_HOST}:{POLARIS_PORT}/api/catalog",
        warehouse=BUCKET,
        credential=f"{POLARIS_ID}:{POLARIS_SECRET}",
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "py-io-impl":        "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint":        S3_ENDPOINT,
            "s3.access-key-id":   MINIO_USER,
            "s3.secret-access-key": MINIO_PASS,
            "s3.path-style-access": "true",
        },
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Drain tickets from data source
    drain_url = f"{DATA_SOURCE_URL}/api/tickets/drain"
    log.info("Draining from %s", drain_url)
    req = urllib.request.Request(drain_url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    rows: list[dict] = payload.get("rows", [])
    log.info("Received %d rows from data source", len(rows))

    # 2. Ensure the Iceberg table exists regardless of whether there are new rows.
    # Polaris is in-memory: after a cluster restart the table entry is lost even
    # though the S3 files remain.  Re-creating here is safe — pyiceberg writes a
    # fresh metadata snapshot.  This also guarantees Trino can see the table
    # before dbt_bronze runs, even on a 0-row cycle.
    from pyiceberg.exceptions import NoSuchTableError
    catalog = get_catalog()
    try:
        iceberg_table = catalog.load_table(ICEBERG_TABLE)
        log.info("Loaded table %s.%s from catalog", *ICEBERG_TABLE)
    except NoSuchTableError:
        log.info("Table %s.%s not found — creating it in catalog", *ICEBERG_TABLE)
        iceberg_table = catalog.create_table(
            identifier=ICEBERG_TABLE,
            schema=SCHEMA,
            location=f"s3://{BUCKET}/warehouse/bronze/raw_tickets",
        )

    if not rows:
        log.info("No new rows — table ensured, pipeline step complete with no writes.")
        return

    # 3. Convert and append
    table = rows_to_table(rows)
    log.info("Built Arrow table: %d rows × %d cols", len(table), len(table.schema))
    iceberg_table.append(table)
    log.info(
        "Appended %d rows to iceberg.%s.%s",
        len(rows), *ICEBERG_TABLE,
    )


if __name__ == "__main__":
    main()
