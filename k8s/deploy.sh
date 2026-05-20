#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy.sh — Deploy the lakehouse lakehouse to Rancher Desktop (k3s)
#
# Usage:
#   cd /path/to/lakehouse
#   ./k8s/deploy.sh [--skip-seed] [--skip-jobs] [--rotate] [--rebuild]
#
#   --rotate   Regenerate credentials, update secrets, rolling-restart services.
#              MySQL/MinIO data is preserved. DB root passwords are not rotated.
#   --rebuild  Delete all deployments/PVCs and redeploy from scratch (new data).
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
INIT_DIR="${PROJECT_DIR}/platform/init"
DAG_DIR="${PROJECT_DIR}/platform/airflow/dags"

NAMESPACE="lakehouse"
CONTEXT="rancher-desktop"

SKIP_SEED=false
SKIP_JOBS=false
ROTATE=false
REBUILD=false
for arg in "$@"; do
    case "${arg}" in
        --skip-seed) SKIP_SEED=true ;;
        --skip-jobs) SKIP_JOBS=true ;;
        --rotate)    ROTATE=true ;;
        --rebuild)   REBUILD=true ;;
        *) echo "Unknown argument: ${arg}" >&2; exit 1 ;;
    esac
done

# ── Logging ───────────────────────────────────────────────────────────────────
BOLD=$'\033[1m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'
RED=$'\033[0;31m'; RESET=$'\033[0m'
STEP=0
log()     { printf '%s[deploy]%s %s\n' "${BOLD}" "${RESET}" "$*"; }
success() { printf '%s[deploy] ✓%s %s\n' "${GREEN}" "${RESET}" "$*"; }
warn()    { printf '%s[deploy] ⚠%s  %s\n' "${YELLOW}" "${RESET}" "$*"; }
fail()    { printf '%s[deploy] ✗%s %s\n' "${RED}" "${RESET}" "$*" >&2; exit 1; }
step()    { (( STEP += 1 )); printf '\n%s══ Step %d: %s%s\n' "${BOLD}" "${STEP}" "$*" "${RESET}"; }

KC="kubectl --context=${CONTEXT} -n ${NAMESPACE}"

# ── 1. Prerequisite checks ────────────────────────────────────────────────────
step "Checking prerequisites"
command -v kubectl >/dev/null || fail "kubectl not found"
command -v python3  >/dev/null || fail "python3 not found"
[[ -f "${PROJECT_DIR}/.env" ]] || fail ".env not found — run: bash init_env.sh"
kubectl --context="${CONTEXT}" cluster-info >/dev/null 2>&1 \
    || fail "Cannot reach Rancher Desktop cluster (context: ${CONTEXT})"
success "kubectl connected to ${CONTEXT}"

# envsubst helper — uses Python to substitute ${VAR} from .env (shell-safe, no export needed)
envsubst_file() {
    local template="$1"
    python3 - "$template" "${PROJECT_DIR}/.env" <<'PYEOF'
import re, sys
env = {}
with open(sys.argv[2]) as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        k, v = line.split('=', 1)
        env[k.strip()] = v.strip()
with open(sys.argv[1]) as f:
    content = f.read()
print(re.sub(r'\${([^}]+)}', lambda m: env.get(m.group(1), ''), content))
PYEOF
}
success ".env ready (python envsubst)"

wait_ready() {
    local deploy="$1"
    local timeout="${2:-180}"
    log "  Waiting for ${deploy} (up to ${timeout}s)..."
    kubectl --context="${CONTEXT}" -n "${NAMESPACE}" \
        wait deployment/"${deploy}" \
        --for=condition=Available \
        --timeout="${timeout}s" \
        || fail "${deploy} did not become ready within ${timeout}s"
    success "${deploy} is ready"
}

# ── --rebuild: tear down existing resources before fresh deploy ───────────────
if [[ "${REBUILD}" == "true" ]]; then
    step "Full rebuild (--rebuild): removing existing cluster resources"
    warn "Deleting all deployments, jobs, PVCs, and ConfigMaps in ${NAMESPACE}..."
    ${KC} delete deployments --all --ignore-not-found
    ${KC} delete jobs        --all --ignore-not-found
    ${KC} delete pvc         --all --ignore-not-found
    ${KC} delete configmaps  --all --ignore-not-found
    log "  Waiting for pods to terminate (up to 120s)..."
    ${KC} wait pods --all --for=delete --timeout=120s 2>/dev/null || true
    success "Resources deleted — continuing with fresh deploy"
fi

# ── --rotate: in-place credential rotation without destroying data ────────────
if [[ "${ROTATE}" == "true" ]]; then
    step "Credential rotation (--rotate)"

    # Save DB root passwords — these are baked into persistent volumes and cannot
    # be changed by simply updating the k8s Secret; they require ALTER USER.
    OLD_MYSQL_ROOT=$(grep '^MYSQL_ROOT_PASSWORD=' "${PROJECT_DIR}/.env" | cut -d= -f2-)
    OLD_POSTGRES_PW=$(grep '^POSTGRES_PASSWORD=' "${PROJECT_DIR}/.env" | cut -d= -f2-)

    log "  Regenerating credentials in .env..."
    bash "${PROJECT_DIR}/init_env.sh" >/dev/null

    # Restore DB root passwords so MySQL/Postgres keep working with their stored state
    python3 - "${PROJECT_DIR}/.env" "${OLD_MYSQL_ROOT}" "${OLD_POSTGRES_PW}" <<'PYEOF'
import re, sys
path, mysql_root, pg_pw = sys.argv[1], sys.argv[2], sys.argv[3]
with open(path) as f:
    content = f.read()
content = re.sub(r'^MYSQL_ROOT_PASSWORD=.*', f'MYSQL_ROOT_PASSWORD={mysql_root}', content, flags=re.M)
content = re.sub(r'^POSTGRES_PASSWORD=.*',  f'POSTGRES_PASSWORD={pg_pw}',         content, flags=re.M)
content = re.sub(
    r'^AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=.*',
    f'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:{pg_pw}@postgres:5432/airflow',
    content, flags=re.M,
)
with open(path, 'w') as f:
    f.write(content)
PYEOF
    NEW_MYSQL_PW=$(grep '^MYSQL_PASSWORD=' "${PROJECT_DIR}/.env" | cut -d= -f2-)
    success "DB root passwords preserved; application credentials refreshed"

    log "  Updating lakehouse-secrets..."
    kubectl --context="${CONTEXT}" -n "${NAMESPACE}" \
        create secret generic lakehouse-secrets \
        --from-env-file="${PROJECT_DIR}/.env" \
        --dry-run=client -o yaml \
        | kubectl --context="${CONTEXT}" apply -f -

    log "  Updating Trino ConfigMap (Polaris OAuth credentials)..."
    envsubst_file "${SCRIPT_DIR}/configmap-trino.yaml" \
        | kubectl --context="${CONTEXT}" apply -f -

    log "  Rotating MySQL 'lakehouse' user password via ALTER USER..."
    ${KC} exec deployment/mysql -- \
        mysql -u root -p"${OLD_MYSQL_ROOT}" \
        -e "ALTER USER 'lakehouse'@'%' IDENTIFIED BY '${NEW_MYSQL_PW}'; FLUSH PRIVILEGES;" \
        2>/dev/null \
        && success "MySQL lakehouse password rotated" \
        || warn "MySQL ALTER USER failed — pod may not be ready; password syncs on next restart"

    log "  Rolling restart: MinIO, Polaris, Trino, Airflow, Metabase..."
    for deploy in minio polaris trino airflow-scheduler airflow-webserver metabase; do
        ${KC} rollout restart deployment/"${deploy}" 2>/dev/null \
            && log "  → ${deploy} restarting" \
            || warn "  → ${deploy} not found (skip)"
    done

    wait_ready polaris          180
    wait_ready trino            300
    wait_ready airflow-webserver 180
    wait_ready metabase         180

    success "Credential rotation complete. MySQL and MinIO data preserved."
    printf '\n%s  New credentials written to .env%s\n\n' "${GREEN}" "${RESET}"
    exit 0
fi

# ── 2. Create namespace ───────────────────────────────────────────────────────
step "Creating namespace ${NAMESPACE}"
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/namespace.yaml"
success "Namespace ready"

# ── 3. Secrets (built from .env — no secret.yaml committed) ──────────────────
step "Creating/updating lakehouse-secrets from .env"
kubectl --context="${CONTEXT}" -n "${NAMESPACE}" \
    create secret generic lakehouse-secrets \
    --from-env-file="${PROJECT_DIR}/.env" \
    --dry-run=client -o yaml \
    | kubectl --context="${CONTEXT}" apply -f -
success "Secrets applied"

# ── 4. PVCs ───────────────────────────────────────────────────────────────────
step "Applying PersistentVolumeClaims"
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/pvc.yaml"
success "PVCs applied"

# ── 5. ConfigMaps ─────────────────────────────────────────────────────────────
step "Applying ConfigMaps"

# Trino config — substitute credentials from .env before applying
envsubst_file "${SCRIPT_DIR}/configmap-trino.yaml" \
    | kubectl --context="${CONTEXT}" apply -f -

# MySQL init SQL
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/configmap-mysql-init.yaml"

# Polaris app config + scope proxy script
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/configmap-polaris.yaml"
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/configmap-scope-proxy.yaml"

# Init scripts (Python) — generated from actual source files
log "  Building init-scripts ConfigMap from platform/init/ ..."
${KC} create configmap init-scripts \
    --from-file=04_generate_tickets.py="${INIT_DIR}/04_generate_tickets.py" \
    --from-file=05_metabase_setup.py="${INIT_DIR}/05_metabase_setup.py" \
    --dry-run=client -o yaml \
    | kubectl --context="${CONTEXT}" apply -f -

# Airflow scripts — fetch_and_ingest + populate_mysql_cache, mounted into Airflow pod
log "  Building airflow-scripts ConfigMap ..."
${KC} create configmap airflow-scripts \
    --from-file=fetch_and_ingest.py="${INIT_DIR}/fetch_and_ingest.py" \
    --from-file=populate_mysql_cache.py="${INIT_DIR}/populate_mysql_cache.py" \
    --dry-run=client -o yaml \
    | kubectl --context="${CONTEXT}" apply -f -

# Data source app — mounted into the data-source pod
log "  Building data-source-app ConfigMap from platform/data_source/ ..."
${KC} create configmap data-source-app \
    --from-file=app.py="${PROJECT_DIR}/platform/data_source/app.py" \
    --dry-run=client -o yaml \
    | kubectl --context="${CONTEXT}" apply -f -

# Polaris bootstrap script
log "  Building polaris-bootstrap-script ConfigMap ..."
${KC} create configmap polaris-bootstrap-script \
    --from-file=bootstrap.py="${INIT_DIR}/02_polaris_bootstrap.py" \
    --dry-run=client -o yaml \
    | kubectl --context="${CONTEXT}" apply -f -

# Airflow DAGs — listed explicitly to avoid ConfigMap subPath issues
log "  Building airflow-dags ConfigMap from platform/airflow/dags/ ..."
${KC} create configmap airflow-dags \
    --from-file=pipeline_daily.py="${DAG_DIR}/pipeline_daily.py" \
    --from-file=pipeline_backfill.py="${DAG_DIR}/pipeline_backfill.py" \
    --from-file=pipeline_hourly.py="${DAG_DIR}/pipeline_hourly.py" \
    --from-file=pipeline_streaming.py="${DAG_DIR}/pipeline_streaming.py" \
    --dry-run=client -o yaml \
    | kubectl --context="${CONTEXT}" apply -f -

success "ConfigMaps applied"

# ── 6. Core data services ──────────────────────────────────────────────────────
step "Deploying core data services (MinIO, MySQL, PostgreSQL)"
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/minio.yaml"
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/mysql.yaml"
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/postgres.yaml"

# ── 7. Polaris ────────────────────────────────────────────────────────────────
step "Deploying Apache Polaris"
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/polaris.yaml"

# ── 8. Wait for core services ──────────────────────────────────────────────────
step "Waiting for core services to become ready"

wait_ready minio     120
wait_ready mysql     180
wait_ready postgres   90

# ── 9. PostgreSQL init job (create polaris database) ──────────────────────────
if [[ "${SKIP_JOBS}" == "false" ]]; then
    step "Initialising PostgreSQL: creating polaris database"
    ${KC} delete job postgres-init --ignore-not-found
    kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/jobs/00-postgres-init.yaml"
    log "  Waiting for postgres-init job to complete..."
    ${KC} wait job/postgres-init --for=condition=complete --timeout=120s \
        || { ${KC} logs job/postgres-init; fail "postgres-init job failed"; }
    success "PostgreSQL polaris database ready"
fi

# ── 10. MinIO init job ────────────────────────────────────────────────────────
if [[ "${SKIP_JOBS}" == "false" ]]; then
    step "Running MinIO bucket initialisation job"
    ${KC} delete job minio-init --ignore-not-found
    kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/jobs/01-minio-init.yaml"
    log "  Waiting for minio-init job to complete..."
    ${KC} wait job/minio-init --for=condition=complete --timeout=120s \
        || { ${KC} logs job/minio-init; fail "minio-init job failed"; }
    success "MinIO bucket lakehouse-local created"
fi

# ── 11. Polaris bootstrap job ─────────────────────────────────────────────────
step "Waiting for Polaris to be ready (PostgreSQL persistence)"
wait_ready polaris 300

if [[ "${SKIP_JOBS}" == "false" ]]; then
    step "Running Polaris bootstrap job"
    ${KC} delete job polaris-bootstrap --ignore-not-found
    kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/jobs/02-polaris-bootstrap.yaml"
    log "  Waiting for polaris-bootstrap job to complete..."
    ${KC} wait job/polaris-bootstrap --for=condition=complete --timeout=180s \
        || { ${KC} logs job/polaris-bootstrap; fail "polaris-bootstrap job failed"; }
    success "Polaris catalog bootstrapped"
fi

# ── 12. Trino ─────────────────────────────────────────────────────────────────
step "Deploying Trino"
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/trino.yaml"
wait_ready trino 300

# ── 13. Data Source pod ────────────────────────────────────────────────────────
step "Deploying data-source pod"
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/data-source.yaml"
wait_ready data-source 60
success "Data source pod ready"

# ── 13b. Generate seed data ───────────────────────────────────────────────────
if [[ "${SKIP_JOBS}" == "false" && "${SKIP_SEED}" == "false" ]]; then
    step "Generating 10M ticket records"
    ${KC} delete job generate-tickets --ignore-not-found
    kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/jobs/04-generate-tickets.yaml"
    log "  Waiting for generate-tickets job (up to 60m)..."
    ${KC} wait job/generate-tickets --for=condition=complete --timeout=3600s \
        || { ${KC} logs job/generate-tickets; fail "generate-tickets job failed"; }
    success "Seed data generated"
elif [[ "${SKIP_SEED}" == "true" ]]; then
    warn "Skipping seed data generation (--skip-seed)"
fi

# ── 14. Airflow ────────────────────────────────────────────────────────────────
step "Deploying Airflow"
envsubst_file "${SCRIPT_DIR}/airflow.yaml" \
    | kubectl --context="${CONTEXT}" apply -f -
log "  Running airflow-init job..."
${KC} wait job/airflow-init --for=condition=complete --timeout=180s \
    || { ${KC} logs job/airflow-init; fail "airflow-init job failed"; }
wait_ready airflow-webserver 180
log "  Unpausing all DAGs..."
for dag in lakehouse_streaming lakehouse_hourly lakehouse_daily lakehouse_backfill; do
    ${KC} exec deployment/airflow-scheduler -- airflow dags unpause "${dag}" 2>/dev/null \
        && log "    ✓ ${dag} unpaused" || log "    ! ${dag} not found (skip)"
done
success "Airflow ready"

# ── 15. Metabase ──────────────────────────────────────────────────────────────
step "Deploying Metabase"
kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/metabase.yaml"
wait_ready metabase 300

if [[ "${SKIP_JOBS}" == "false" ]]; then
    step "Configuring Metabase dashboard"
    ${KC} delete job metabase-setup --ignore-not-found
    kubectl --context="${CONTEXT}" apply -f "${SCRIPT_DIR}/jobs/05-metabase-setup.yaml"
    log "  Waiting for metabase-setup job..."
    ${KC} wait job/metabase-setup --for=condition=complete --timeout=300s \
        || { ${KC} logs job/metabase-setup; fail "metabase-setup job failed"; }
    success "Metabase dashboard configured"
fi

# ── 16. Summary ────────────────────────────────────────────────────────────────
printf '\n%s══════════════════════════════════════════════════════%s\n' "${BOLD}" "${RESET}"
printf '%s  Lakehouse K8s Stack — Service URLs%s\n' "${GREEN}" "${RESET}"
printf '%s══════════════════════════════════════════════════════%s\n\n' "${BOLD}" "${RESET}"
printf '  %-22s %s\n' "MinIO API"       "http://localhost:30900"
printf '  %-22s %s\n' "MinIO Console"   "http://localhost:30901"
printf '  %-22s %s\n' "Trino UI"        "http://localhost:30080"
printf '  %-22s %s\n' "Airflow UI"      "http://localhost:30888  (admin / see .env AIRFLOW_ADMIN_PASSWORD)"
printf '  %-22s %s\n' "Metabase"        "http://localhost:30300  (admin@local.com / see .env METABASE_ADMIN_PASSWORD)"
printf '  %-22s %s\n' "Data Source"     "http://data-source:8080 (cluster-internal — generates 5-20 tickets/5min)"
printf '\n%s  All services deployed.%s\n\n' "${GREEN}" "${RESET}"
printf '  To watch pod status:\n'
printf '    kubectl --context=rancher-desktop -n lakehouse get pods -w\n\n'
printf '  To re-run a job:\n'
printf '    kubectl --context=rancher-desktop -n lakehouse delete job <name>\n'
printf '    kubectl --context=rancher-desktop apply -f k8s/jobs/<name>.yaml\n\n'
