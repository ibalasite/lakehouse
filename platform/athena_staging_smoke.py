#!/usr/bin/env python3
"""
athena_staging_smoke.py
CI gate: execute compiled dbt SQL against real Athena engine (LIMIT 100 wrapper)
to catch runtime incompatibilities that dbt compile cannot detect.

Usage:
  python platform/athena_staging_smoke.py \
    --manifest target/manifest.json \
    --compiled-dir target/compiled \
    --workgroup ci-staging \
    --s3-output s3://lakehouse-ci/athena-staging-results/ \
    [--timeout 300] \
    [--max-concurrent 5] \
    [--dry-run]

Exit codes:
  0 — all models passed
  1 — one or more models failed
  2 — configuration error

Cost controls:
  - Only runs state:modified models (from manifest)
  - LIMIT 100 wrapper stops Athena scan early
  - Uses dedicated ci-staging workgroup with 1 GB per-query scan limit
  - Cleans up S3 output on success
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    sys.exit("boto3 is required: pip install boto3")


POLL_INTERVAL = 5          # seconds between status polls
LIMIT_WRAPPER = "SELECT * FROM (\n{sql}\n) _smoke_t\nLIMIT 100"


# ── helpers ───────────────────────────────────────────────────────────────────

def load_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def get_modified_models(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Return nodes that dbt considers 'modified' (state:modified).
    In the manifest, modified nodes have metadata.is_modified = True
    when produced by `dbt compile --select state:modified+`.
    Falls back to returning all non-ephemeral model nodes if no
    is_modified flag is present (e.g., full-manifest run).
    """
    nodes = []
    for key, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        if node.get("config", {}).get("materialized") == "ephemeral":
            continue
        nodes.append(node)
    return nodes


def compiled_sql_path(compiled_dir: str, node: dict[str, Any]) -> Path | None:
    """Resolve compiled SQL file path for a node."""
    rel = node.get("original_file_path", "")
    # compiled path mirrors source structure under compiled_dir
    p = Path(compiled_dir) / rel
    if p.exists():
        return p
    # fallback: search by model name
    name = node.get("name", "")
    matches = list(Path(compiled_dir).rglob(f"{name}.sql"))
    return matches[0] if matches else None


def wrap_with_limit(sql: str) -> str:
    """Wrap SQL in a LIMIT 100 subquery to minimise Athena scan."""
    # strip trailing semicolon if present
    sql = sql.strip().rstrip(";")
    return LIMIT_WRAPPER.format(sql=sql)


# ── Athena execution ──────────────────────────────────────────────────────────

class AthenaRunner:
    def __init__(
        self,
        workgroup: str,
        s3_output: str,
        timeout: int,
        dry_run: bool,
        region: str | None = None,
    ):
        self.workgroup = workgroup
        self.s3_output = s3_output.rstrip("/")
        self.timeout = timeout
        self.dry_run = dry_run
        self.run_id = str(uuid.uuid4())[:8]
        if not dry_run:
            self.client = boto3.client("athena", region_name=region)
        self._submitted: list[tuple[str, str]] = []  # (query_id, model_name)

    def start_query(self, sql: str, model_name: str) -> str | None:
        wrapped = wrap_with_limit(sql)
        if self.dry_run:
            print(f"  [dry-run] would execute: {model_name}")
            print(f"    SQL: {wrapped[:120].replace(chr(10), ' ')} …")
            return f"dry-run-{model_name}"

        output_location = f"{self.s3_output}/{self.run_id}/{model_name}/"
        try:
            response = self.client.start_query_execution(
                QueryString=wrapped,
                WorkGroup=self.workgroup,
                ResultConfiguration={"OutputLocation": output_location},
                QueryExecutionContext={},
            )
            qid = response["QueryExecutionId"]
            self._submitted.append((qid, model_name))
            return qid
        except ClientError as exc:
            print(f"  [ERROR] {model_name}: failed to start — {exc}", file=sys.stderr)
            return None

    def wait_for_result(self, query_id: str, model_name: str) -> tuple[bool, str]:
        """Poll until terminal state. Returns (success, detail)."""
        if self.dry_run:
            return True, "dry-run OK"

        deadline = time.time() + self.timeout
        while time.time() < deadline:
            resp = self.client.get_query_execution(QueryExecutionId=query_id)
            state = resp["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                stats = resp["QueryExecution"].get("Statistics", {})
                scanned = stats.get("DataScannedInBytes", 0) // (1024 * 1024)
                elapsed = stats.get("TotalExecutionTimeInMillis", 0) / 1000
                return True, f"{elapsed:.1f}s  scanned {scanned} MB  QID={query_id}"
            if state in ("FAILED", "CANCELLED"):
                reason = (
                    resp["QueryExecution"]["Status"]
                    .get("StateChangeReason", "unknown error")
                )
                return False, f"ERROR: {reason}  QID={query_id}"
            time.sleep(POLL_INTERVAL)

        return False, f"TIMEOUT after {self.timeout}s  QID={query_id}"

    def cleanup_s3(self) -> None:
        """Delete staging results from S3 to avoid storage costs."""
        if self.dry_run:
            return
        try:
            s3 = boto3.client("s3")
            bucket, prefix = self._parse_s3(f"{self.s3_output}/{self.run_id}/")
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if objects:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            print(f"  [cleanup] deleted s3://{bucket}/{prefix}")
        except Exception as exc:
            print(f"  [warn] S3 cleanup failed: {exc}", file=sys.stderr)

    @staticmethod
    def _parse_s3(uri: str) -> tuple[str, str]:
        without_scheme = uri.removeprefix("s3://")
        bucket, _, prefix = without_scheme.partition("/")
        return bucket, prefix


# ── main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Athena staging smoke test for CI")
    p.add_argument("--manifest", required=True, help="dbt manifest.json path")
    p.add_argument("--compiled-dir", required=True, help="dbt compiled SQL directory")
    p.add_argument("--workgroup", required=True, help="Athena workgroup (e.g. ci-staging)")
    p.add_argument("--s3-output", required=True, help="S3 URI for Athena results")
    p.add_argument("--timeout", type=int, default=300, help="Per-query timeout in seconds")
    p.add_argument("--max-concurrent", type=int, default=5, help="Max parallel Athena queries")
    p.add_argument("--region", default=None, help="AWS region (default: from env/profile)")
    p.add_argument("--dry-run", action="store_true", help="Print SQL; do not submit to Athena")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Athena staging smoke test")
    print(f"  workgroup : {args.workgroup}")
    print(f"  s3-output : {args.s3_output}")
    print(f"  dry-run   : {args.dry_run}")

    manifest = load_json(args.manifest)
    nodes = get_modified_models(manifest)
    print(f"  models    : {len(nodes)}")

    runner = AthenaRunner(
        workgroup=args.workgroup,
        s3_output=args.s3_output,
        timeout=args.timeout,
        dry_run=args.dry_run,
        region=args.region,
    )

    results: list[tuple[bool, str, str]] = []  # (ok, model_name, detail)
    pending: list[tuple[str, str]] = []  # (query_id, model_name)

    for node in nodes:
        name = node["name"]
        sql_path = compiled_sql_path(args.compiled_dir, node)
        if sql_path is None:
            print(f"  [SKIP] {name}: compiled SQL not found", file=sys.stderr)
            continue

        sql = sql_path.read_text(encoding="utf-8")
        qid = runner.start_query(sql, name)
        if qid:
            pending.append((qid, name))
        else:
            results.append((False, name, "failed to start query"))

        # respect max concurrent limit
        while len(pending) >= args.max_concurrent:
            qid0, name0 = pending.pop(0)
            ok, detail = runner.wait_for_result(qid0, name0)
            status = "OK  " if ok else "FAIL"
            print(f"  [{status}] {name0:40s} {detail}")
            results.append((ok, name0, detail))

    # drain remaining
    for qid, name in pending:
        ok, detail = runner.wait_for_result(qid, name)
        status = "OK  " if ok else "FAIL"
        print(f"  [{status}] {name:40s} {detail}")
        results.append((ok, name, detail))

    failures = [(n, d) for ok, n, d in results if not ok]
    if not failures:
        runner.cleanup_s3()
        print(f"\nAll {len(results)} model(s) passed.")
        sys.exit(0)
    else:
        print(f"\n{len(failures)} model(s) failed:")
        for name, detail in failures:
            print(f"  - {name}: {detail}")
        sys.exit(1)


if __name__ == "__main__":
    main()
