#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_all.sh
# Master initialisation script for the local data lakehouse stack.
#
# What it does (in order):
#   1. Validates prerequisites (Docker, docker compose, .env)
#   2. Sources .env so all credentials are available as env vars
#   3. Starts all Docker Compose services
#   4. Waits for core services to become healthy
#   5. Runs MinIO bucket initialisation  (01_minio_init.sh)
#   6. Runs Polaris bootstrap            (02_polaris_bootstrap.py)
#   7. Applies MySQL schema              (03_mysql_init.sql)
#   8. Optionally generates seed data    (04_generate_tickets.py)
#   9. Optionally runs dbt               (dbt deps && dbt run)
#  10. Prints service URL summary
#
# Usage:
#   cd /path/to/lakehouse
#   ./platform/init/run_all.sh [--skip-seed] [--skip-dbt]
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Resolve script and project directories ────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
INIT_DIR="${SCRIPT_DIR}"

# ── Parse flags ───────────────────────────────────────────────────────────────
SKIP_SEED=false
SKIP_DBT=false
for arg in "$@"; do
    case "${arg}" in
        --skip-seed) SKIP_SEED=true ;;
        --skip-dbt)  SKIP_DBT=true  ;;
        *) echo "Unknown argument: ${arg}" >&2; exit 1 ;;
    esac
done

# ── Logging ───────────────────────────────────────────────────────────────────
BOLD=$'\033[1m'
GREEN=$'\033[0;32m'
YELLOW=$'\033[0;33m'
RED=$'\033[0;31m'
RESET=$'\033[0m'

log()     { printf '%s[run_all]%s %s\n' "${BOLD}" "${RESET}" "$*"; }
success() { printf '%s[run_all] ✓%s %s\n' "${GREEN}" "${RESET}" "$*"; }
warn()    { printf '%s[run_all] ⚠%s  %s\n' "${YELLOW}" "${RESET}" "$*"; }
fail()    { printf '%s[run_all] ✗%s %s\n' "${RED}" "${RESET}" "$*" >&2; exit 1; }

# ── Step counter ──────────────────────────────────────────────────────────────
STEP=0
step() {
    (( STEP += 1 ))
    printf '\n%s══ Step %d: %s%s\n' "${BOLD}" "${STEP}" "$*" "${RESET}"
}

# ── 1. Prerequisite checks ────────────────────────────────────────────────────
step "Checking prerequisites"

command -v docker > /dev/null 2>&1 \
    || fail "docker is not installed or not in PATH"
docker info > /dev/null 2>&1 \
    || fail "Docker daemon is not running. Start Docker Desktop and retry."
success "Docker is running"

docker compose version > /dev/null 2>&1 \
    || fail "'docker compose' (v2 plugin) is not available"
success "docker compose plugin available"

command -v python3 > /dev/null 2>&1 \
    || fail "python3 is required for bootstrap scripts"
success "python3 available"

ENV_FILE="${PROJECT_DIR}/.env"
if [[ ! -f "${ENV_FILE}" ]]; then
    fail ".env not found at ${ENV_FILE}. Copy .env.example to .env and fill in real values."
fi
if [[ ! -r "${ENV_FILE}" ]]; then
    fail ".env exists but is not readable by the current user."
fi
# Reject world-readable .env files — secrets must not be globally visible.
# Obtain the octal permission bits in a portable way:
#   macOS stat : -f '%A' returns symbolic string (e.g. -rw-------); use -f '%Lp' for octal
#   GNU stat   : -c '%a' returns octal directly (e.g. 600)
# We normalise to a 3-digit octal string then inspect the last digit (world bits).
if stat -c '%a' "${ENV_FILE}" > /dev/null 2>&1; then
    # GNU stat (Linux)
    ENV_PERMS="$(stat -c '%a' "${ENV_FILE}")"
elif stat -f '%Lp' "${ENV_FILE}" > /dev/null 2>&1; then
    # BSD/macOS stat — %Lp gives octal permission bits without file-type prefix
    ENV_PERMS="$(stat -f '%Lp' "${ENV_FILE}")"
else
    fail "Unable to determine .env file permissions. Neither GNU nor BSD stat produced usable output."
fi
if [[ -z "${ENV_PERMS}" ]]; then
    fail "stat returned empty permissions for .env. Cannot verify file safety."
fi
# Pad to at least 3 digits so the regex is unambiguous (e.g. "600" not "6").
ENV_PERMS_PADDED="$(printf '%03d' "$(( 10#${ENV_PERMS} ))")"
# The rightmost octal digit encodes world (other) permissions.
# Any digit 1-7 means the file is accessible to other users — reject it.
if [[ "${ENV_PERMS_PADDED}" =~ [0-9][0-9][1-7]$ ]]; then
    fail ".env has world-accessible permissions (mode ${ENV_PERMS_PADDED}). Run: chmod 600 .env"
fi
success ".env file present and permissions are safe (${ENV_PERMS_PADDED})"

# ── 2. Load environment variables ─────────────────────────────────────────────
step "Loading environment variables from .env"
# Reject lines that are not comments, blank, or KEY=value assignments.
# This prevents sourcing .env files that contain arbitrary shell commands.
while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -z "${line}" || "${line}" =~ ^[[:space:]]*# ]] && continue
    if [[ ! "${line}" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; then
        fail ".env contains a line that is not a valid KEY=value assignment: ${line}"
    fi
done < "${ENV_FILE}"
# Only source after validation passes.
set -a
# shellcheck source=/dev/null
source "${ENV_FILE}"
set +a
success "Environment loaded"

# Verify required variables are present (fail fast before starting containers).
REQUIRED_VARS=(
    MINIO_ROOT_USER MINIO_ROOT_PASSWORD
    MYSQL_ROOT_PASSWORD MYSQL_USER MYSQL_PASSWORD
    POSTGRES_USER POSTGRES_PASSWORD
    AIRFLOW__CORE__FERNET_KEY
    POLARIS_CLIENT_ID POLARIS_CLIENT_SECRET
)
for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var:-}" ]]; then
        fail "Required variable ${var} is empty in .env"
    fi
done
success "All required variables present"

# ── 3. Start Docker Compose stack ─────────────────────────────────────────────
step "Starting Docker Compose stack"
cd "${PROJECT_DIR}"
docker compose up -d --remove-orphans
success "Docker Compose stack started"

# ── 4. Wait for core services ─────────────────────────────────────────────────
step "Waiting for core services to become healthy"

wait_healthy() {
    local service="$1"
    local max_wait="${2:-180}"
    local elapsed=0
    log "  Waiting for '${service}' (up to ${max_wait}s) ..."
    until [[ "$(docker inspect --format='{{.State.Health.Status}}' "${service}" 2>/dev/null)" == "healthy" ]]; do
        if (( elapsed >= max_wait )); then
            fail "Service '${service}' did not become healthy within ${max_wait}s"
        fi
        sleep 5
        (( elapsed += 5 ))
    done
    success "${service} is healthy (${elapsed}s)"
}

wait_healthy "minio"      120
wait_healthy "mysql"      120
wait_healthy "postgres"   120
wait_healthy "polaris"    180
wait_healthy "trino"      240

# ── 5. MinIO bucket initialisation ───────────────────────────────────────────
step "Initialising MinIO buckets"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-lakehouse}"
bash "${INIT_DIR}/01_minio_init.sh"
success "MinIO buckets ready"

# ── 6. Polaris bootstrap ──────────────────────────────────────────────────────
step "Bootstrapping Apache Polaris catalog"

# Install requests if not already present (non-destructive).
if ! python3 -c "import requests" 2>/dev/null; then
    log "  Installing 'requests' library ..."
    python3 -m pip install --quiet requests
fi

python3 "${INIT_DIR}/02_polaris_bootstrap.py"
success "Polaris catalog and namespaces ready"

# ── 7. MySQL schema ───────────────────────────────────────────────────────────
step "Applying MySQL schema (03_mysql_init.sql)"
# Pipe SQL through docker exec; credentials passed via env-flags, never
# interpolated into a shell command string.
docker exec -i mysql \
    mysql \
        --user=root \
        --password="${MYSQL_ROOT_PASSWORD}" \
        --default-character-set=utf8mb4 \
    < "${INIT_DIR}/03_mysql_init.sql"
success "MySQL schema applied"

# ── 8. Seed data (optional) ───────────────────────────────────────────────────
step "Generating seed ticket data"
SEED_SCRIPT="${INIT_DIR}/04_generate_tickets.py"
if [[ "${SKIP_SEED}" == "true" ]]; then
    warn "Skipping seed data generation (--skip-seed)"
elif [[ ! -f "${SEED_SCRIPT}" ]]; then
    warn "Seed script not found: ${SEED_SCRIPT} — skipping"
else
    python3 "${SEED_SCRIPT}"
    success "Seed data generated"
fi

# ── 9. dbt (optional) ─────────────────────────────────────────────────────────
step "Running dbt models"
DBT_DIR="${PROJECT_DIR}/dbt"
if [[ "${SKIP_DBT}" == "true" ]]; then
    warn "Skipping dbt run (--skip-dbt)"
elif [[ ! -d "${DBT_DIR}" ]]; then
    warn "dbt project directory not found at ${DBT_DIR} — skipping"
elif command -v dbt > /dev/null 2>&1; then
    (
        cd "${DBT_DIR}"
        dbt deps
        # Bronze → Silver → Gold (Iceberg)
        dbt run --target local --exclude cache_ticket_daily
        # Cache → MySQL (via Trino MySQL catalog connector)
        dbt run --target mysql_cache --select cache_ticket_daily
    )
    success "dbt models executed"
else
    warn "dbt not found in PATH — skipping. Install with: pip install dbt-trino"
fi

# ── 9b. Metabase auto-setup ────────────────────────────────────────────────────
step "Configuring Metabase dashboards"
METABASE_SCRIPT="${INIT_DIR}/05_metabase_setup.py"
if [[ ! -f "${METABASE_SCRIPT}" ]]; then
    warn "Metabase setup script not found — skipping"
else
    if ! python3 -c "import requests" 2>/dev/null; then
        python3 -m pip install --quiet requests
    fi
    wait_healthy "metabase" 180
    python3 "${METABASE_SCRIPT}"
    success "Metabase dashboard ready"
fi

# ── 10. Summary ───────────────────────────────────────────────────────────────
printf '\n%s══════════════════════════════════════════════════════%s\n' "${BOLD}" "${RESET}"
printf '%s  Local Lakehouse Stack — Service URLs%s\n' "${GREEN}" "${RESET}"
printf '%s══════════════════════════════════════════════════════%s\n\n' "${BOLD}" "${RESET}"
printf '  %-20s %s\n' "MinIO API"      "http://localhost:${MINIO_API_PORT:-9000}"
printf '  %-20s %s\n' "MinIO Console"  "http://localhost:${MINIO_CONSOLE_PORT:-9001}"
printf '  %-20s %s\n' "Polaris Catalog" "http://localhost:${POLARIS_PORT:-8181}"
printf '  %-20s %s\n' "Polaris Mgmt"   "http://localhost:${POLARIS_MGMT_PORT:-8182}"
printf '  %-20s %s\n' "Trino UI"       "http://localhost:${TRINO_PORT:-8080}"
printf '  %-20s %s\n' "Airflow UI"     "http://localhost:${AIRFLOW_PORT:-8888}"
printf '  %-20s %s\n' "Metabase"       "http://localhost:${METABASE_PORT:-3000}"
printf '  %-20s %s\n' "MySQL"          "localhost:${MYSQL_PORT:-3306}  db=${MYSQL_DATABASE}"
printf '  %-20s %s\n' "PostgreSQL"     "localhost:${POSTGRES_PORT:-5432}  db=${POSTGRES_DB}"
printf '\n%s  All services are up and ready.%s\n\n' "${GREEN}" "${RESET}"
