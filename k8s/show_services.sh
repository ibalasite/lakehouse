#!/usr/bin/env bash
# show_services.sh — Print all service URLs and current credentials.
# Reads live credentials from .env in the project root.
# Called automatically at the end of deploy.sh; also callable at any time.
#
# Usage:
#   ./k8s/show_services.sh           # standalone — shows current creds
#   bash k8s/show_services.sh        # explicit bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${PROJECT_DIR}/.env"

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "ERROR: .env not found at ${ENV_FILE}" >&2
    echo "Run: bash init_env.sh" >&2
    exit 1
fi

# Read a single key from .env using Python (handles special characters in values)
_env() {
    python3 - "${ENV_FILE}" "$1" <<'PYEOF'
import sys
key = sys.argv[2]
with open(sys.argv[1]) as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        k, v = line.split('=', 1)
        if k.strip() == key:
            print(v.strip())
PYEOF
}

MINIO_USER=$(_env MINIO_ROOT_USER)
MINIO_PASS=$(_env MINIO_ROOT_PASSWORD)
AIRFLOW_PASS=$(_env AIRFLOW_ADMIN_PASSWORD)
METABASE_EMAIL=$(_env METABASE_ADMIN_EMAIL 2>/dev/null || echo "admin@local.com")
METABASE_PASS=$(_env METABASE_ADMIN_PASSWORD)
MYSQL_USER=$(_env MYSQL_USER)
MYSQL_PASS=$(_env MYSQL_PASSWORD)
POLARIS_ID=$(_env POLARIS_CLIENT_ID)
POLARIS_SECRET=$(_env POLARIS_CLIENT_SECRET)

BOLD=$'\033[1m'; GREEN=$'\033[0;32m'; CYAN=$'\033[0;36m'
YELLOW=$'\033[0;33m'; DIM=$'\033[2m'; RESET=$'\033[0m'

printf '\n'
printf '%s╔══════════════════════════════════════════════════════════════════╗%s\n' "${BOLD}" "${RESET}"
printf '%s║  Lakehouse K8s Stack — Service Reference                        ║%s\n' "${GREEN}" "${RESET}"
printf '%s╚══════════════════════════════════════════════════════════════════╝%s\n' "${BOLD}" "${RESET}"
printf '\n'

# ── WEB UIs ───────────────────────────────────────────────────────────────────
printf '%s── WEB UIs (browser access)%s\n' "${CYAN}" "${RESET}"
printf '\n'
printf '  %-18s %s\n' "Metabase"        "http://localhost:30300"
printf '  %-18s %s\n' "  user:"         "${METABASE_EMAIL}"
printf '  %-18s %s\n' "  pass:"         "${METABASE_PASS}"
printf '\n'
printf '  %-18s %s\n' "Airflow"         "http://localhost:30888"
printf '  %-18s %s\n' "  user:"         "admin"
printf '  %-18s %s\n' "  pass:"         "${AIRFLOW_PASS}"
printf '\n'
printf '  %-18s %s\n' "MinIO Console"   "http://localhost:30901"
printf '  %-18s %s\n' "  user:"         "${MINIO_USER}"
printf '  %-18s %s\n' "  pass:"         "${MINIO_PASS}"
printf '\n'
printf '  %-18s %s\n' "Trino UI"        "http://localhost:30080  (no login)"
printf '  %-18s %s\n' "MinIO S3 API"    "http://localhost:30900  (S3 endpoint — no browser login)"
printf '\n'

# ── Pod / kubectl access ───────────────────────────────────────────────────────
printf '%s── Pod access (kubectl)%s\n' "${CYAN}" "${RESET}"
printf '\n'
printf '  %s# Trino interactive shell%s\n' "${DIM}" "${RESET}"
printf '  kubectl -n lakehouse exec deployment/trino -- trino\n'
printf '\n'
printf '  %s# MySQL — lakehouse user%s\n' "${DIM}" "${RESET}"
printf '  %-18s %s\n' "  user:"         "${MYSQL_USER}"
printf '  %-18s %s\n' "  pass:"         "${MYSQL_PASS}"
printf '  kubectl -n lakehouse exec deployment/mysql -- \\\n'
printf '    mysql -u%s -p'"'"'%s'"'"' lakehouse_cache\n' "${MYSQL_USER}" "${MYSQL_PASS}"
printf '\n'
printf '  %s# Airflow scheduler shell%s\n' "${DIM}" "${RESET}"
printf '  kubectl -n lakehouse exec -it deployment/airflow-scheduler -- bash\n'
printf '\n'

# ── ClusterIP / internal services ─────────────────────────────────────────────
printf '%s── ClusterIP services (port-forward or kubectl exec)%s\n' "${CYAN}" "${RESET}"
printf '\n'
printf '  %-18s %s\n' "Polaris API"     "http://polaris:8181  (cluster-internal)"
printf '  %-18s %s\n' "  client_id:"    "${POLARIS_ID}"
printf '  %-18s %s\n' "  client_secret:" "${POLARIS_SECRET}"
printf '  %s# Port-forward Polaris to host:%s\n' "${DIM}" "${RESET}"
printf '  kubectl -n lakehouse port-forward svc/polaris 8181:8181 &\n'
printf '\n'
printf '  %-18s %s\n' "Data Source"     "http://datasource:8080  (cluster-internal)"
printf '  %s# Check buffer size:%s\n' "${DIM}" "${RESET}"
printf '  kubectl -n lakehouse exec deployment/airflow-scheduler -- \\\n'
printf '    python3 -c "import urllib.request,json; r=urllib.request.urlopen('"'"'http://datasource:8080/health'"'"'); print(json.load(r))"\n'
printf '\n'

# ── Quick status ──────────────────────────────────────────────────────────────
printf '%s── Quick status%s\n' "${CYAN}" "${RESET}"
printf '\n'
printf '  kubectl -n lakehouse get pods\n'
printf '  kubectl -n lakehouse get pods -w   %s# watch live%s\n' "${DIM}" "${RESET}"
printf '\n'
printf '%s  Credentials are in: %s.env%s\n' "${YELLOW}" "${PROJECT_DIR}/" "${RESET}"
printf '%s  Regenerate with:    bash %s/init_env.sh%s\n' "${DIM}" "${PROJECT_DIR}" "${RESET}"
printf '\n'
