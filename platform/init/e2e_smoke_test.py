#!/usr/bin/env python3
"""
e2e_smoke_test.py
=================
End-to-end pipeline smoke test for the Local Data Lakehouse.

Pipeline flow:
  [Local]   Generate today's hourly ticket data  →  Iceberg (via pyiceberg)
  [Airflow] lakehouse_hourly DAG:
              Trino aggregate query (today)       →  MySQL cache_ticket_daily
              Trino aggregate query (today)       →  MySQL cache_ticket_hourly
              Metabase dashboard card query       →  verify rows returned
  [Verify]  MySQL today row count                →  ✓
  [Verify]  Metabase dashboard card              →  ✓

Port-forward requirements (auto-started if missing):
  svc/polaris  8181 → 8181  (Iceberg REST catalog)
  svc/mysql    3306 → 3306  (MySQL cache DB)

NodePorts used directly (no port-forward needed):
  Trino    localhost:30080
  Airflow  localhost:30888
  Metabase localhost:30300
  MinIO    localhost:30900

Usage:
    pip install 'pyiceberg[s3]' pyarrow mysql-connector-python requests
    python3 e2e_smoke_test.py
    python3 e2e_smoke_test.py --rows 200 --timeout 600
    python3 e2e_smoke_test.py --skip-generate   # skip Iceberg write
    python3 e2e_smoke_test.py --skip-airflow    # run cache refresh locally
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import socket
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("e2e_smoke")


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        log.error("Required environment variable %s is not set. Run: source .env", name)
        sys.exit(1)
    return value


# ── Config ─────────────────────────────────────────────────────────────────────
# Local (port-forward or NodePort)
POLARIS_HOST          = os.environ.get("POLARIS_HOST", "localhost")
POLARIS_PORT          = int(os.environ.get("POLARIS_PORT", "8181"))
POLARIS_CLIENT_ID     = _require_env("POLARIS_CLIENT_ID")
POLARIS_CLIENT_SECRET = _require_env("POLARIS_CLIENT_SECRET")

MINIO_HOST = os.environ.get("MINIO_HOST", "localhost")
MINIO_PORT = int(os.environ.get("MINIO_API_PORT", "30900"))        # NodePort
MINIO_USER = _require_env("MINIO_ROOT_USER")
MINIO_PASS = _require_env("MINIO_ROOT_PASSWORD")

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "30080"))            # NodePort

MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "lakehouse")
MYSQL_PASS = _require_env("MYSQL_PASSWORD")
MYSQL_DB   = os.environ.get("MYSQL_DATABASE", "lakehouse_cache")

AIRFLOW_HOST = os.environ.get("AIRFLOW_HOST", "localhost")
AIRFLOW_PORT = int(os.environ.get("AIRFLOW_PORT", "30888"))        # NodePort
AIRFLOW_USER = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_PASS = _require_env("AIRFLOW_ADMIN_PASSWORD")

METABASE_HOST = os.environ.get("METABASE_HOST", "localhost")
METABASE_PORT = int(os.environ.get("METABASE_PORT", "30300"))      # NodePort
METABASE_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@local.com")
METABASE_PASS  = os.environ.get("METABASE_ADMIN_PASSWORD", "")

K8S_CONTEXT   = "rancher-desktop"
K8S_NAMESPACE = "lakehouse"
AIRFLOW_DAG   = "lakehouse_hourly"

BOLD  = "\033[1m"
GREEN = "\033[0;32m"
RED   = "\033[0;31m"
YELLOW = "\033[0;33m"
RESET = "\033[0m"


# ── Port utilities ─────────────────────────────────────────────────────────────

def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        return s.connect_ex((host, port)) == 0


def _wait_port(host: str, port: int, label: str, max_wait: int = 15) -> None:
    for _ in range(max_wait):
        if _port_open(host, port):
            return
        time.sleep(1)
    raise RuntimeError(f"{label} ({host}:{port}) did not open within {max_wait}s")


def start_port_forward(svc: str, local_port: int, remote_port: int) -> subprocess.Popen:
    """Start kubectl port-forward and return the process handle."""
    cmd = [
        "kubectl", f"--context={K8S_CONTEXT}", f"-n={K8S_NAMESPACE}",
        "port-forward", f"svc/{svc}", f"{local_port}:{remote_port}",
    ]
    log.info("Starting port-forward: %s", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _wait_port("localhost", local_port, svc)
    log.info("  Port-forward %s:%d established (pid %d)", svc, local_port, proc.pid)
    return proc


# ── Step 1: Ensure connectivity ────────────────────────────────────────────────

def ensure_ports() -> list[subprocess.Popen]:
    """Open port-forwards for services that don't have NodePorts."""
    procs: list[subprocess.Popen] = []

    if not _port_open(POLARIS_HOST, POLARIS_PORT):
        procs.append(start_port_forward("polaris", POLARIS_PORT, 8181))
    else:
        log.info("Polaris port %d already open", POLARIS_PORT)

    if not _port_open(MYSQL_HOST, MYSQL_PORT):
        procs.append(start_port_forward("mysql", MYSQL_PORT, 3306))
    else:
        log.info("MySQL port %d already open", MYSQL_PORT)

    # Verify NodePorts
    for label, host, port in [
        ("Trino",    TRINO_HOST,    TRINO_PORT),
        ("Airflow",  AIRFLOW_HOST,  AIRFLOW_PORT),
        ("Metabase", METABASE_HOST, METABASE_PORT),
        ("MinIO",    MINIO_HOST,    MINIO_PORT),
    ]:
        if _port_open(host, port):
            log.info("%s port %d reachable", label, port)
        else:
            raise RuntimeError(
                f"{label} not reachable at {host}:{port}. "
                "Is Rancher Desktop running with the lakehouse stack?"
            )

    return procs


# ── Step 2: Generate hourly ticket data ───────────────────────────────────────

def generate_today_tickets(n_rows: int = 500) -> None:
    """Append n_rows synthetic ticket records for TODAY into Iceberg."""
    try:
        import numpy as np
        import pyarrow as pa
        import pyiceberg.catalog.rest as _pir
        from pyiceberg.catalog.rest import RestCatalog
    except ImportError as e:
        raise SystemExit(
            f"Missing package: {e}\n"
            "Install with: pip install 'pyiceberg[s3]' pyarrow numpy"
        ) from e

    # Patch: remove X-Iceberg-Access-Delegation header (Polaris compatibility)
    _orig = _pir.RestCatalog.__init__
    def _patched(self, *args, **kwargs):
        _orig(self, *args, **kwargs)
        self._session.headers.pop("X-Iceberg-Access-Delegation", None)
    _pir.RestCatalog.__init__ = _patched

    log.info("Connecting to Polaris at %s:%d", POLARIS_HOST, POLARIS_PORT)
    catalog = RestCatalog(
        name="polaris",
        uri=f"http://{POLARIS_HOST}:{POLARIS_PORT}/api/catalog",
        warehouse="lakehouse",
        credential=f"{POLARIS_CLIENT_ID}:{POLARIS_CLIENT_SECRET}",
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": f"http://{MINIO_HOST}:{MINIO_PORT}",
            "s3.access-key-id": MINIO_USER,
            "s3.secret-access-key": MINIO_PASS,
            "s3.path-style-access": "true",
        },
    )

    table = catalog.load_table("raw.raw_tickets")
    log.info("Loaded table raw.raw_tickets — generating %d rows for today", n_rows)

    # Generate timestamps spread across today's hours (UTC)
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    rng = np.random.default_rng(int(now.timestamp()))
    size = n_rows

    # prblm_sysdate: spread across today's hours up to now
    elapsed_secs = int((now - today_start).total_seconds()) or 3600
    sysdate_sec = (
        int(today_start.timestamp())
        + rng.integers(0, elapsed_secs, size=size)
    )

    update_delta    = rng.integers(0, 3600, size=size)
    updateddate_sec = sysdate_sec + update_delta

    STATUS_IDS = [1,2,3,4,5]; STATUS_W = [0.05,0.08,0.07,0.05,0.75]
    status_ids  = rng.choice(STATUS_IDS, size=size, p=STATUS_W)
    is_resolved = status_ids == 5

    done_sec = updateddate_sec + rng.integers(0, 7200, size=size)
    donedate_us = np.where(is_resolved, done_sec.astype(np.int64) * 1_000_000, -1)

    preassign_end_sec   = sysdate_sec + rng.integers(7200, 86400, size=size)
    preassign_begin_sec = preassign_end_sec - rng.integers(0, 1800, size=size)
    ingested_sec        = sysdate_sec + rng.integers(0, 3600, size=size)

    SOURCE_IDS = [1,2,3,4,5]; SOURCE_W = [0.35,0.25,0.20,0.15,0.05]
    CATSUB_IDS = list(range(1,11)); CATSUB_W = [0.30,0.20,0.15,0.05,0.05,0.10,0.05,0.04,0.03,0.03]
    CLASS_W = np.array([1/(i**0.5) for i in range(1,21)], float); CLASS_W /= CLASS_W.sum()

    source_ids   = rng.choice(SOURCE_IDS, size=size, p=SOURCE_W)
    class_ids    = rng.choice(np.arange(1,21), size=size, p=CLASS_W).astype(np.int32)
    intclass_ids = rng.integers(1, 31, size=size).astype(np.int32)
    catsub_ids   = rng.choice(CATSUB_IDS, size=size, p=CATSUB_W)
    supplier_ids = rng.integers(1, 5001, size=size).astype(np.int32)

    perf_roll   = rng.random(size=size)
    perform_ids = np.where(perf_roll < 0.60, -1, np.where(perf_roll < 0.90, 1, 2)).astype(np.int32)

    comp_roll    = rng.random(size=size)
    complain_ids = np.where(comp_roll < 0.85, 1, np.where(comp_roll < 0.95, 2, 3)).astype(np.int32)

    done_ata   = is_resolved & (rng.random(size=size) < 0.40)
    has_fwd    = rng.random(size=size) < 0.20
    fwd_sec    = sysdate_sec + rng.integers(3600, 7*86400, size=size)
    fwd_type   = np.where(has_fwd, rng.integers(1, 6, size=size), -1).astype(np.int32)

    has_notasreason  = rng.random(size=size) < 0.30
    notasreason_raw  = rng.integers(1, 21, size=size)
    has_notasowndept = rng.random(size=size) < 0.30
    notasowndept_raw = rng.integers(1, 16, size=size).astype(np.int32)

    NAMES = [f"USER-{i:06d}" for i in range(10000)]
    names        = np.array(random.choices(NAMES, k=size), dtype=object)
    processusers = np.array([f"EMP-{rng.integers(1000,9999)}" for _ in range(size)], dtype=object)
    doneusers    = np.array([f"EMP-{rng.integers(1000,9999)}" for _ in range(size)], dtype=object)
    sysusers     = np.array([f"EMP-{rng.integers(1000,9999)}" for _ in range(size)], dtype=object)
    usr_ids      = np.array([f"USR-{rng.integers(0,1000000):07d}" for _ in range(size)], dtype=object)
    gd_ids       = np.array([f"GD-{rng.integers(0,100000):06d}" for _ in range(size)], dtype=object)

    year = now.year
    counter = Counter()
    codes = []
    for i in range(size):
        counter[year] += 1
        codes.append(f"PRB-{year}-SMOKE-{counter[year]:06d}")

    def ts_us(arr: np.ndarray) -> pa.Array:
        return pa.array((arr * 1_000_000).astype(np.int64).tolist(), type=pa.timestamp("us", tz="UTC"))

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

    batch = pa.table({
        "prblm_code":               pa.array(codes, type=pa.string()),
        "prblm_sysdate":            ts_us(sysdate_sec),
        "prblm_updateddate":        ts_us(updateddate_sec),
        "prblm_donedate":           nullable_ts(donedate_us, is_resolved),
        "prblm_preassignenddate":   ts_us(preassign_end_sec),
        "prblm_preassignbegindate": ts_us(preassign_begin_sec),
        "prblm_status_id":          pa.array(status_ids.astype(np.int32).tolist(), type=pa.int32()),
        "prblm_source_id":          pa.array(source_ids.astype(np.int32).tolist(), type=pa.int32()),
        "prblm_class_id":           pa.array(class_ids.tolist(), type=pa.int32()),
        "prblm_intclass_id":        pa.array(intclass_ids.tolist(), type=pa.int32()),
        "prblm_perform_id":         pa.array(perform_ids.tolist(), type=pa.int32()),
        "prblm_complain_id":        pa.array(complain_ids.astype(np.int32).tolist(), type=pa.int32()),
        "prblm_processuser":        pa.array(processusers.tolist(), type=pa.string()),
        "prblm_doneuser":           pa.array(doneusers.tolist(), type=pa.string()),
        "prblm_sysuser":            pa.array(sysusers.tolist(), type=pa.string()),
        "usr_id":                   pa.array(usr_ids.tolist(), type=pa.string()),
        "prblm_name":               pa.array(names.tolist(), type=pa.string()),
        "gd_id":                    pa.array(gd_ids.tolist(), type=pa.string()),
        "catsub_id":                pa.array(catsub_ids.astype(np.int32).tolist(), type=pa.int32()),
        "supplier_id":              pa.array(supplier_ids.tolist(), type=pa.int32()),
        "prblm_doneatatime":        pa.array(done_ata.tolist(), type=pa.bool_()),
        "prblm_forwarddatetime":    nullable_ts((fwd_sec * 1_000_000).astype(np.int64), has_fwd),
        "prblm_forwardtype":        nullable_int32(fwd_type, has_fwd),
        "prblm_notasreason_id":     pa.array(
            [str(int(v)) if m else None for v, m in zip(notasreason_raw.tolist(), has_notasreason.tolist())],
            type=pa.string(),
        ),
        "prblm_notasowndept_id":    nullable_int32(notasowndept_raw, has_notasowndept),
        "ingested_at":              ts_us(ingested_sec),
    })

    t0 = time.monotonic()
    table.append(batch)
    elapsed = time.monotonic() - t0
    log.info("Appended %d rows to Iceberg in %.2fs", size, elapsed)


# ── Step 3: Trigger Airflow DAG ───────────────────────────────────────────────

def trigger_airflow_dag(dag_id: str) -> str:
    import requests as req
    url = f"http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/api/v1/dags/{dag_id}/dagRuns"
    conf = {"logical_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    r = req.post(url, json=conf, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=30)
    if r.status_code == 404:
        raise RuntimeError(
            f"DAG '{dag_id}' not found in Airflow. "
            "Deploy the DAG and ensure the scheduler has loaded it. "
            "Run: kubectl --context=rancher-desktop -n lakehouse rollout restart deployment/airflow-scheduler"
        )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Airflow trigger failed {r.status_code}: {r.text[:300]}")
    run_id = r.json()["dag_run_id"]
    log.info("Triggered DAG '%s' → run_id: %s", dag_id, run_id)
    return run_id


def wait_for_dag(dag_id: str, run_id: str, timeout: int = 600) -> str:
    """Poll Airflow until DAG run reaches a terminal state, return final state."""
    import requests as req
    url = f"http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        r = req.get(url, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=30)
        if r.status_code != 200:
            log.warning("Poll returned %d", r.status_code)
            time.sleep(10)
            continue
        state = r.json().get("state", "")
        elapsed = time.monotonic() - t0
        log.info("  [%.0fs] DAG state: %s", elapsed, state)
        if state in ("success", "failed"):
            return state
        time.sleep(15)
    return "timeout"


# ── Step 4: Local fallback — run cache refresh directly ───────────────────────

def refresh_cache_locally() -> None:
    """Run hourly + daily cache refresh locally (without Airflow)."""
    try:
        import mysql.connector
        import trino.dbapi
    except ImportError as e:
        raise SystemExit(f"Missing package: {e}\nInstall: pip install 'trino>=0.330' mysql-connector-python") from e

    log.info("Running local cache refresh (Trino → MySQL) for today …")

    HOURLY_Q = """
    SELECT
        CAST(DATE_TRUNC('day',  prblm_sysdate) AS DATE)  AS date_sk,
        CAST(HOUR(prblm_sysdate) AS INTEGER)              AS hour_of_day,
        catsub_id, prblm_source_id, prblm_class_id,
        prblm_perform_id, prblm_status_id,
        COUNT(*),
        COUNT(*) FILTER (WHERE prblm_status_id = 5),
        COUNT(*) FILTER (WHERE prblm_doneatatime = TRUE),
        COUNT(*) FILTER (WHERE prblm_complain_id >= 2),
        COUNT(*) FILTER (WHERE prblm_forwardtype IS NOT NULL),
        COUNT(*) FILTER (WHERE prblm_doneatatime = TRUE AND prblm_status_id = 5),
        AVG(CASE WHEN prblm_donedate IS NOT NULL
                 THEN DATE_DIFF('minute', prblm_sysdate, prblm_donedate) / 60.0 END),
        AVG(DATE_DIFF('minute', prblm_sysdate, prblm_preassignenddate) / 60.0)
    FROM iceberg.bronze.raw_tickets
    WHERE CAST(DATE_TRUNC('day', prblm_sysdate) AS DATE) = CURRENT_DATE
    GROUP BY 1,2,3,4,5,6,7
    ORDER BY 1,2,3
    """

    HOURLY_INSERT = """
    INSERT INTO cache_ticket_hourly
        (date_sk, hour_of_day, catsub_id, prblm_source_id, prblm_class_id,
         prblm_perform_id, prblm_status_id,
         total_tickets, resolved_tickets, one_shot_resolved,
         complain_tickets, forwarded_tickets, within_sla_tickets,
         avg_resolution_hours, avg_response_hours)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    t0 = time.monotonic()
    conn_t = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="admin",
        catalog="iceberg", schema="raw", http_scheme="http", request_timeout=120,
    )
    cur = conn_t.cursor()
    cur.execute(HOURLY_Q)
    rows = cur.fetchall()
    conn_t.close()
    log.info("Trino returned %d hourly rows for today", len(rows))

    if not rows:
        log.warning("No rows for today in Iceberg — was data generation successful?")
        return

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


# ── Step 5: Verify MySQL ───────────────────────────────────────────────────────

def verify_mysql() -> int:
    """Return count of today's rows in cache_ticket_hourly."""
    try:
        import mysql.connector
    except ImportError as e:
        raise SystemExit(f"Missing: {e}") from e

    conn = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASS, database=MYSQL_DB,
    )
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM cache_ticket_hourly WHERE date_sk = CURDATE()")
    count = cur.fetchone()[0]
    conn.close()
    return count


# ── Step 6: Verify Metabase ────────────────────────────────────────────────────

def verify_metabase() -> tuple[bool, int]:
    """Login to Metabase, query dashboard card, return (ok, row_count)."""
    try:
        import requests as req
    except ImportError as e:
        raise SystemExit(f"Missing: {e}") from e

    base = f"http://{METABASE_HOST}:{METABASE_PORT}"

    r = req.post(f"{base}/api/session",
                  json={"username": METABASE_EMAIL, "password": METABASE_PASS},
                  timeout=30)
    if r.status_code != 200:
        log.error("Metabase login failed: %d %s", r.status_code, r.text[:200])
        return False, 0
    token = r.json()["id"]
    headers = {"X-Metabase-Session": token}

    r2 = req.get(f"{base}/api/dashboard/2", headers=headers, timeout=30)
    if r2.status_code != 200:
        log.error("Dashboard fetch failed: %d", r2.status_code)
        return False, 0

    dash = r2.json()
    dashcards = dash.get("dashcards", [])
    log.info("Dashboard '%s' has %d card(s)", dash.get("name"), len(dashcards))

    total_rows = 0
    for dc in dashcards:
        card_id = dc.get("card_id")
        if not card_id:
            continue
        r3 = req.post(f"{base}/api/card/{card_id}/query",
                       headers=headers, timeout=60)
        if r3.status_code in (200, 202):
            rows = r3.json().get("data", {}).get("rows", [])
            total_rows += len(rows)
            log.info("  Card %d → %d rows", card_id, len(rows))
        else:
            log.warning("  Card %d query returned %d", card_id, r3.status_code)

    return total_rows > 0, total_rows


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Lakehouse e2e smoke test")
    parser.add_argument("--rows",          type=int, default=500,
                        help="Number of synthetic ticket rows to generate (default 500)")
    parser.add_argument("--timeout",       type=int, default=600,
                        help="Airflow DAG poll timeout in seconds (default 600)")
    parser.add_argument("--skip-generate", action="store_true",
                        help="Skip Iceberg data generation step")
    parser.add_argument("--skip-airflow",  action="store_true",
                        help="Skip Airflow and run cache refresh locally instead")
    args = parser.parse_args()

    print(f"\n{BOLD}══ Lakehouse E2E Smoke Test ══{RESET}")
    print(f"  rows={args.rows}  timeout={args.timeout}s  "
          f"skip-generate={args.skip_generate}  skip-airflow={args.skip_airflow}\n")

    results: list[tuple[str, bool, str]] = []
    pf_procs: list[subprocess.Popen] = []

    try:
        # ── Connectivity ────────────────────────────────────────────────────────
        log.info("Step 1: Checking service connectivity and port-forwards …")
        pf_procs = ensure_ports()
        results.append(("Connectivity", True, "All services reachable"))

        # ── Generate data ────────────────────────────────────────────────────────
        if args.skip_generate:
            log.info("Step 2: Skipping data generation (--skip-generate)")
            results.append(("Data generation", True, "skipped"))
        else:
            log.info("Step 2: Generating %d ticket rows for today → Iceberg …", args.rows)
            t_gen = time.monotonic()
            generate_today_tickets(args.rows)
            results.append(("Data generation", True,
                             f"{args.rows} rows in {time.monotonic()-t_gen:.1f}s"))

        # ── Airflow OR local refresh ─────────────────────────────────────────────
        if args.skip_airflow:
            log.info("Step 3: Refreshing MySQL cache locally (--skip-airflow) …")
            refresh_cache_locally()
            results.append(("Cache refresh (local)", True, "done"))
        else:
            log.info("Step 3: Triggering Airflow DAG '%s' …", AIRFLOW_DAG)
            try:
                run_id = trigger_airflow_dag(AIRFLOW_DAG)
                log.info("Step 4: Polling Airflow DAG (timeout %ds) …", args.timeout)
                state = wait_for_dag(AIRFLOW_DAG, run_id, timeout=args.timeout)
                ok = state == "success"
                results.append((f"Airflow DAG ({AIRFLOW_DAG})", ok,
                                 f"final state: {state}"))
                if not ok:
                    log.error("DAG did not succeed (state=%s). "
                              "Check Airflow UI at http://localhost:%d", state, AIRFLOW_PORT)
            except RuntimeError as exc:
                log.error("Airflow step failed: %s", exc)
                results.append(("Airflow DAG", False, str(exc)[:120]))
                log.info("Falling back to local cache refresh …")
                refresh_cache_locally()
                results.append(("Cache refresh (fallback)", True, "done"))

        # ── Verify MySQL ─────────────────────────────────────────────────────────
        log.info("Step 5: Verifying MySQL cache_ticket_hourly (today) …")
        mysql_count = verify_mysql()
        ok = mysql_count > 0
        msg = f"{mysql_count:,} rows for today"
        results.append(("MySQL hourly cache", ok, msg))
        if ok:
            log.info("  MySQL: %s", msg)
        else:
            log.error("  MySQL: no rows for today in cache_ticket_hourly")

        # ── Verify Metabase ──────────────────────────────────────────────────────
        log.info("Step 6: Verifying Metabase dashboard …")
        mb_ok, mb_rows = verify_metabase()
        results.append(("Metabase dashboard", mb_ok,
                         f"{mb_rows} total rows across cards" if mb_ok else "no rows returned"))

    finally:
        # Clean up any port-forwards we started
        for proc in pf_procs:
            proc.terminate()
            log.info("Stopped port-forward pid %d", proc.pid)

    # ── Summary ──────────────────────────────────────────────────────────────────
    print(f"\n{BOLD}══ Smoke Test Results ══{RESET}")
    all_ok = True
    for label, ok, detail in results:
        icon  = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
        color = GREEN if ok else RED
        print(f"  [{icon}] {color}{label}{RESET}: {detail}")
        if not ok:
            all_ok = False

    if all_ok:
        print(f"\n{GREEN}{BOLD}All checks passed.{RESET}")
        print(f"  Dashboard: http://localhost:{METABASE_PORT}/dashboard/2\n")
    else:
        print(f"\n{RED}{BOLD}Some checks FAILED — see logs above.{RESET}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
