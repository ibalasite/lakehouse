#!/usr/bin/env python3
"""
02_polaris_bootstrap.py
-----------------------
Bootstraps Apache Polaris for the local lakehouse stack.

Steps performed:
  1. Wait for the Polaris management API to be ready
  2. Obtain an OAuth2 token using bootstrap credentials
  3. Create a service principal (trino-svc)
  4. Create the 'lakehouse' catalog (INTERNAL type, S3 base location)
  5. Create a catalog role 'all_access' and grant ALL privileges
  6. Create a principal role and assign it to trino-svc
  7. Create namespaces: raw, bronze, silver, gold, cache

Credentials are read exclusively from environment variables.
No secrets are hard-coded in this file.

Usage:
    # source .env first, or set variables manually:
    source .env
    python3 02_polaris_bootstrap.py
"""

import os
import sys
import time
import logging

import requests
from requests.exceptions import ConnectionError, Timeout

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[polaris-bootstrap] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ── Configuration (all from environment — no hard-coded secrets) ─────────────
def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        log.error("Required environment variable %s is not set.", name)
        sys.exit(1)
    return value


POLARIS_CATALOG_BASE = os.environ.get(
    "POLARIS_CATALOG_URL", "http://localhost:8181/api/catalog/v1"
)
POLARIS_MGMT_BASE = os.environ.get(
    "POLARIS_MGMT_URL", "http://localhost:8182/management/v1"
)
CLIENT_ID = _require_env("POLARIS_CLIENT_ID")
CLIENT_SECRET = _require_env("POLARIS_CLIENT_SECRET")

CATALOG_NAME = os.environ.get("POLARIS_CATALOG", "lakehouse")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "lakehouse-local")
PRINCIPAL_NAME = "trino-svc"
CATALOG_ROLE_NAME = "all_access"
PRINCIPAL_ROLE_NAME = "all_access_role"
# EDD section 6.1: four required namespaces — raw is NOT in the spec.
# Source data lands in bronze.raw_tickets (append-only).
NAMESPACES = ["bronze", "silver", "gold", "cache"]

MAX_WAIT_SECONDS = int(os.environ.get("POLARIS_WAIT_SECONDS", "120"))


# ── Wait helper ───────────────────────────────────────────────────────────────
def wait_for_polaris() -> None:
    """Poll the Polaris management base URL until it responds."""
    log.info("Waiting for Polaris management API at %s ...", POLARIS_MGMT_BASE)
    deadline = time.monotonic() + MAX_WAIT_SECONDS
    attempt = 0
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"{POLARIS_MGMT_BASE}/", timeout=5)
            if r.status_code < 500:
                log.info("Polaris is ready (HTTP %s).", r.status_code)
                return
        except (ConnectionError, Timeout):
            pass
        attempt += 1
        if attempt % 5 == 1:
            remaining = int(deadline - time.monotonic())
            log.info("  Still waiting ... attempt %d (%ds remaining)", attempt, remaining)
        time.sleep(3)
    log.error("Polaris did not become ready within %ds.", MAX_WAIT_SECONDS)
    sys.exit(1)


# ── Token acquisition ─────────────────────────────────────────────────────────
def get_token() -> str:
    """
    Request an OAuth2 client_credentials token from Polaris.
    CLIENT_SECRET is never logged.
    """
    log.info("Obtaining OAuth2 token ...")
    resp = requests.post(
        f"{POLARIS_CATALOG_BASE}/oauth/tokens",
        # Use data= (form-encoded) as required by OAuth2 client_credentials.
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        timeout=15,
    )
    if not resp.ok:
        log.error("Token request failed: HTTP %s — %s", resp.status_code, resp.text)
        sys.exit(1)
    token = resp.json().get("access_token")
    if not token:
        log.error("Token response missing 'access_token': %s", resp.json())
        sys.exit(1)
    log.info("Token obtained.")
    return token


# ── API helpers ───────────────────────────────────────────────────────────────
def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def post(token: str, path: str, payload: dict, *, allow_conflict: bool = True) -> dict:
    url = f"{POLARIS_MGMT_BASE}{path}"
    resp = requests.post(url, json=payload, headers=_headers(token), timeout=15)
    if resp.status_code == 409 and allow_conflict:
        log.info("  Already exists (409) — skipping: %s", path)
        return {}
    if not resp.ok:
        log.error("POST %s failed: HTTP %s — %s", url, resp.status_code, resp.text)
        sys.exit(1)
    log.info("  POST %s -> %s", path, resp.status_code)
    return resp.json() if resp.content else {}


def put(token: str, path: str, payload: dict) -> dict:
    url = f"{POLARIS_MGMT_BASE}{path}"
    resp = requests.put(url, json=payload, headers=_headers(token), timeout=15)
    if not resp.ok:
        log.error("PUT %s failed: HTTP %s — %s", url, resp.status_code, resp.text)
        sys.exit(1)
    log.info("  PUT %s -> %s", path, resp.status_code)
    return resp.json() if resp.content else {}


def catalog_post(token: str, path: str, payload: dict) -> dict:
    """POST to the Iceberg Catalog REST API (not management API)."""
    url = f"{POLARIS_CATALOG_BASE}{path}"
    resp = requests.post(url, json=payload, headers=_headers(token), timeout=15)
    if resp.status_code == 409:
        log.info("  Already exists (409) — skipping: %s", path)
        return {}
    if not resp.ok:
        log.error("POST %s failed: HTTP %s — %s", url, resp.status_code, resp.text)
        sys.exit(1)
    log.info("  POST %s -> %s", path, resp.status_code)
    return resp.json() if resp.content else {}


# ── Bootstrap steps ───────────────────────────────────────────────────────────
def create_principal(token: str) -> None:
    log.info("Step 1: Creating service principal '%s' ...", PRINCIPAL_NAME)
    post(token, "/principals", {
        "name": PRINCIPAL_NAME,
        "type": "SERVICE",
        "properties": {},
    })


def create_catalog(token: str) -> None:
    log.info("Step 2: Creating catalog '%s' (base: s3://%s/warehouse/) ...", CATALOG_NAME, MINIO_BUCKET)
    post(token, "/catalogs", {
        "name": CATALOG_NAME,
        "type": "INTERNAL",
        "properties": {
            # EDD section 6.2: s3://lakehouse-local/warehouse/<namespace>/...
            "default-base-location": f"s3://{MINIO_BUCKET}/warehouse/",
        },
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": [f"s3://{MINIO_BUCKET}/"],
            "pathStyleAccess": True,
        },
    })


def create_catalog_role(token: str) -> None:
    log.info("Step 3: Creating catalog role '%s' ...", CATALOG_ROLE_NAME)
    post(token, f"/catalogs/{CATALOG_NAME}/catalog-roles", {
        "name": CATALOG_ROLE_NAME,
    })


def grant_catalog_privileges(token: str) -> None:
    log.info("Step 4: Granting ALL privileges on catalog '%s' to role '%s' ...",
             CATALOG_NAME, CATALOG_ROLE_NAME)
    privilege_grants = [
        "CATALOG_MANAGE_CONTENT",
        "CATALOG_MANAGE_METADATA",
        "CATALOG_READ_PROPERTIES",
        "CATALOG_WRITE_PROPERTIES",
    ]
    for privilege in privilege_grants:
        put(token, f"/catalogs/{CATALOG_NAME}/catalog-roles/{CATALOG_ROLE_NAME}/grants", {
            "grant": {
                "type": "catalog",
                "privilege": privilege,
            }
        })


def create_principal_role(token: str) -> None:
    log.info("Step 5: Creating principal role '%s' ...", PRINCIPAL_ROLE_NAME)
    post(token, "/principal-roles", {
        "principalRole": {"name": PRINCIPAL_ROLE_NAME},
    })


def assign_principal_role(token: str) -> None:
    log.info("Step 6: Assigning principal role to '%s' ...", PRINCIPAL_NAME)
    put(token, f"/principals/{PRINCIPAL_NAME}/principal-roles", {
        "principalRole": {"name": PRINCIPAL_ROLE_NAME},
    })


def assign_catalog_role(token: str) -> None:
    log.info("Step 7: Assigning catalog role to principal role ...")
    put(token, f"/principal-roles/{PRINCIPAL_ROLE_NAME}/catalog-roles/{CATALOG_NAME}", {
        "catalogRole": {"name": CATALOG_ROLE_NAME},
    })


def create_namespaces(token: str) -> None:
    log.info("Step 8: Creating namespaces %s ...", NAMESPACES)
    for ns in NAMESPACES:
        catalog_post(token, f"/{CATALOG_NAME}/namespaces", {
            "namespace": [ns],
            "properties": {
                # EDD section 6.2: warehouse/ prefix per namespace
                "location": f"s3://{MINIO_BUCKET}/warehouse/{ns}/",
            },
        })


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    wait_for_polaris()
    token = get_token()

    create_principal(token)
    create_catalog(token)
    create_catalog_role(token)
    grant_catalog_privileges(token)
    create_principal_role(token)
    assign_principal_role(token)
    assign_catalog_role(token)
    create_namespaces(token)

    log.info("")
    log.info("Polaris bootstrap complete.")
    log.info("  Catalog    : %s", CATALOG_NAME)
    log.info("  Principal  : %s", PRINCIPAL_NAME)
    log.info("  Namespaces : %s", ", ".join(NAMESPACES))


if __name__ == "__main__":
    main()
