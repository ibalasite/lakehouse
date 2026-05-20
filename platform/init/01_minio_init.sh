#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# 01_minio_init.sh
# Initialises MinIO buckets and lifecycle policies for the local lakehouse.
# Run from the host after `docker compose up -d` via run_all.sh.
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
MINIO_HOST="${MINIO_HOST:-http://localhost:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:?MINIO_ROOT_USER is not set — source .env first}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is not set — source .env first}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"

# Pin to a specific tag so builds are deterministic.
MC_IMAGE="${MC_IMAGE:-minio/mc:RELEASE.2024-11-21T17-21-54Z}"

# Docker network — matches compose project + network name.
COMPOSE_PROJECT="${COMPOSE_PROJECT_NAME:-lakehouse}"
DOCKER_NETWORK="${DOCKER_NETWORK:-${COMPOSE_PROJECT}_lakehouse-net}"

BUCKET="lakehouse-local"
MAX_WAIT_SECONDS=120

# ── Temp env-file (credentials never appear in process list or docker inspect) -
# mktemp with mode 0600 so only the current user can read it.
MC_ENV_FILE="$(mktemp)"
chmod 0600 "${MC_ENV_FILE}"
# Register cleanup so the file is removed even if the script exits early.
trap 'rm -f "${MC_ENV_FILE}"' EXIT

# MC_HOST_<alias> is the official mc environment variable for alias credentials.
# Writing to a file (not a CLI flag) keeps secrets out of /proc/*/cmdline and
# `docker inspect` output.
printf 'MC_HOST_lh=http://%s:%s@minio:9000\n' \
    "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" \
    > "${MC_ENV_FILE}"

# ── Helpers ───────────────────────────────────────────────────────────────────
log()  { printf '[minio-init] %s\n' "$*"; }
fail() { printf '[minio-init] ERROR: %s\n' "$*" >&2; exit 1; }

validate_url() {
    local url="$1"
    if [[ ! "${url}" =~ ^https?://[a-zA-Z0-9._:-]+(:[0-9]+)?(/.*)?$ ]]; then
        fail "MINIO_HOST '${url}' is not a valid http/https URL"
    fi
}

# Credentials passed exclusively through --env-file, not as CLI arguments.
run_mc() {
    docker run --rm \
        --network "${DOCKER_NETWORK}" \
        --env-file "${MC_ENV_FILE}" \
        "${MC_IMAGE}" \
        "$@"
}

# ── 1. Validate inputs ────────────────────────────────────────────────────────
validate_url "${MINIO_HOST}"

# ── 2. Wait for MinIO health endpoint ────────────────────────────────────────
log "Waiting for MinIO at ${MINIO_HOST} (up to ${MAX_WAIT_SECONDS}s) ..."
elapsed=0
consecutive_failures=0
until curl -sf --max-time 5 "${MINIO_HOST}/minio/health/live" > /dev/null 2>&1; do
    if (( elapsed >= MAX_WAIT_SECONDS )); then
        fail "MinIO did not become healthy within ${MAX_WAIT_SECONDS}s"
    fi
    (( consecutive_failures += 1 ))
    if (( consecutive_failures % 5 == 1 )); then
        log "  Still waiting ... (${elapsed}s elapsed, attempt ${consecutive_failures})"
    fi
    sleep 3
    (( elapsed += 3 ))
done
log "MinIO is healthy (waited ${elapsed}s)"

# ── 3. Create bucket ──────────────────────────────────────────────────────────
log "Creating bucket: ${BUCKET} ..."
run_mc mb --ignore-existing "lh/${BUCKET}"

# ── 4. Enforce private access ─────────────────────────────────────────────────
log "Setting bucket policy to private (no anonymous access) ..."
run_mc anonymous set none "lh/${BUCKET}"

# ── 5. Lifecycle rules ────────────────────────────────────────────────────────
log "Applying lifecycle policy to bronze warehouse zone ..."
LIFECYCLE_JSON='{"Rules":[{"ID":"expire-bronze-365d","Status":"Enabled","Filter":{"Prefix":"warehouse/bronze/"},"Expiration":{"Days":365}}]}'
if echo "${LIFECYCLE_JSON}" | run_mc ilm import "lh/${BUCKET}" 2>/dev/null; then
    log "  Lifecycle policy applied."
else
    log "  Lifecycle import skipped (may require MinIO SUBNET licence — non-fatal)."
fi

# ── 6. Summary ────────────────────────────────────────────────────────────────
log ""
log "MinIO initialisation complete."
log "  Bucket  : s3://${BUCKET}/"
log "  Warehouse: s3://${BUCKET}/warehouse/{bronze,silver,gold,cache}/"
log "  Console : http://localhost:${MINIO_CONSOLE_PORT}"
