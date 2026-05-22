#!/usr/bin/env python3
"""
data_source/app.py
==================
Synthetic data source pod — simulates an upstream ticket system.

Behaviour
---------
  • A background thread generates DATA_SOURCE_MIN_ROWS – DATA_SOURCE_MAX_ROWS
    ticket rows every DATA_SOURCE_INTERVAL_SEC seconds (default: 5–20 rows per
    300 seconds).
  • GET /api/tickets/drain  returns all buffered rows as JSON and clears the
    buffer, so each Airflow fetch gets only the new rows since the last drain.
  • GET /health             returns {"status":"ok","buffered_rows":<n>}.

Only Python stdlib is used so the pod can run on the plain python:3.11-slim
image without any pip install step.

All generated values mirror the raw_tickets Iceberg schema so fetch_and_ingest.py
can write them directly without field mapping.
"""

from __future__ import annotations

import http.server
import json
import logging
import os
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="[data-source] %(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("data_source")

# ── Configuration ─────────────────────────────────────────────────────────────
LISTEN_PORT   = int(os.environ.get("APP_PORT",              "8080"))
INTERVAL_SEC  = int(os.environ.get("APP_INTERVAL_SEC",     "300"))
MIN_ROWS      = int(os.environ.get("APP_MIN_ROWS",         "5"))
MAX_ROWS      = int(os.environ.get("APP_MAX_ROWS",         "20"))

# ── Thread-safe buffer ────────────────────────────────────────────────────────
_lock:   threading.Lock = threading.Lock()
_buffer: deque[dict]    = deque()

# ── Reference distributions (pure Python, no NumPy) ──────────────────────────
STATUS_IDS = [1, 2, 3, 4, 5]
STATUS_W   = [0.05, 0.08, 0.07, 0.05, 0.75]

SOURCE_IDS = [1, 2, 3, 4, 5]
SOURCE_W   = [0.35, 0.25, 0.20, 0.15, 0.05]

CATSUB_IDS = list(range(1, 11))
CATSUB_W   = [0.30, 0.20, 0.15, 0.05, 0.05, 0.10, 0.05, 0.04, 0.03, 0.03]

PERFORM_IDS = [1, 2, 99]
PERFORM_W   = [0.10, 0.05, 0.85]

_rng = random.Random()


def _choice(population: list[int], weights: list[float]) -> int:
    return _rng.choices(population, weights=weights, k=1)[0]


def _isoformat(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _from_epoch(epoch_sec: float) -> datetime:
    return datetime.fromtimestamp(epoch_sec, tz=timezone.utc)


def _generate_ticket(now: datetime) -> dict:
    now_sec      = now.timestamp()
    status_id    = _choice(STATUS_IDS, STATUS_W)
    is_resolved  = status_id == 5

    update_sec   = now_sec + _rng.randint(0, 30 * 86400)
    done_sec     = update_sec + _rng.randint(0, 7200) if is_resolved else None
    pre_end_sec  = now_sec + _rng.randint(7200, 86400)
    pre_beg_sec  = pre_end_sec - _rng.randint(0, 1800)
    has_forward  = _rng.random() < 0.20
    fwd_sec      = now_sec + _rng.randint(3600, 7 * 86400) if has_forward else None

    prblm_code = f"TKT-{now.year}-{_rng.randint(1_000_000, 9_999_999)}"

    return {
        "prblm_code":               prblm_code,
        "prblm_sysdate":            _isoformat(now),
        "prblm_updateddate":        _isoformat(_from_epoch(update_sec)),
        "prblm_donedate":           _isoformat(_from_epoch(done_sec)) if done_sec else None,
        "prblm_preassignenddate":   _isoformat(_from_epoch(pre_end_sec)),
        "prblm_preassignbegindate": _isoformat(_from_epoch(pre_beg_sec)),
        "prblm_status_id":          status_id,
        "prblm_source_id":          _choice(SOURCE_IDS, SOURCE_W),
        "prblm_class_id":           _rng.randint(1, 20),
        "prblm_intclass_id":        _rng.randint(1, 30),
        "prblm_perform_id":         _choice(PERFORM_IDS, PERFORM_W),
        "prblm_complain_id":        (lambda r: 1 if r < 0.85 else (2 if r < 0.95 else 3))(_rng.random()),
        "prblm_processuser":        f"EMP-{_rng.randint(1000, 9999):04d}",
        "prblm_doneuser":           f"EMP-{_rng.randint(1000, 9999):04d}" if is_resolved else None,
        "prblm_sysuser":            f"EMP-{_rng.randint(1000, 9999):04d}",
        "usr_id":                   f"USR-{_rng.randint(0, 999_999):07d}",
        "prblm_name":               f"問題單 {prblm_code}",
        "gd_id":                    f"GD-{_rng.randint(0, 99_999):06d}",
        "catsub_id":                _choice(CATSUB_IDS, CATSUB_W),
        "supplier_id":              _rng.randint(1, 100),
        "prblm_doneatatime":        is_resolved and _rng.random() < 0.30,
        "prblm_forwarddatetime":    _isoformat(_from_epoch(fwd_sec)) if fwd_sec else None,
        "prblm_forwardtype":        1 if has_forward else 0,
        "prblm_notasreason_id":     f"R{_rng.randint(1, 20):02d}",
        "prblm_notasowndept_id":    _rng.randint(1, 10),
        "ingested_at":              _isoformat(now),
    }


# ── Background generator ──────────────────────────────────────────────────────

def _generator_thread() -> None:
    log.info(
        "Generator started: %d–%d rows every %ds",
        MIN_ROWS, MAX_ROWS, INTERVAL_SEC,
    )
    while True:
        time.sleep(INTERVAL_SEC)
        n   = _rng.randint(MIN_ROWS, MAX_ROWS)
        now = datetime.now(tz=timezone.utc)
        tickets = [_generate_ticket(now) for _ in range(n)]
        with _lock:
            _buffer.extend(tickets)
        log.info(
            "Generated %d tickets — buffer size: %d",
            n, len(_buffer),
        )


# ── HTTP handler ──────────────────────────────────────────────────────────────

class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:
        pass  # suppress per-request noise; generator thread logs enough

    def _json(self, status: int, body: object) -> None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type",   "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path == "/health":
            with _lock:
                n = len(_buffer)
            self._json(200, {"status": "ok", "buffered_rows": n})

        elif self.path in ("/api/tickets/drain", "/api/tickets"):
            with _lock:
                rows = list(_buffer)
                _buffer.clear()
            log.info("Drain: returned %d rows to caller", len(rows))
            self._json(200, {"rows": rows, "count": len(rows)})

        else:
            self._json(404, {"error": "not found", "path": self.path})


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    threading.Thread(target=_generator_thread, daemon=True).start()
    server = http.server.HTTPServer(("0.0.0.0", LISTEN_PORT), _Handler)
    log.info("Listening on 0.0.0.0:%d", LISTEN_PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
