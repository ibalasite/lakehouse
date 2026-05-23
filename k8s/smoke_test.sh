#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# smoke_test.sh — Post-deploy acceptance verification
#
# Usage:
#   ./k8s/smoke_test.sh
#
# Run AFTER deploy.sh. Verifies the critical Trino→Polaris→MinIO chain,
# iceberg schema presence, data-source API, and Airflow DAG state.
# Exit 0 = all checks passed. Exit 1 = at least one check failed.
#
# EDD references: §14 Phase 1 驗收, §16.1 smoke test, AC-01/03/04/06
# ─────────────────────────────────────────────────────────────────────────────

set -uo pipefail

CONTEXT="rancher-desktop"
NAMESPACE="lakehouse"
KC="kubectl --context=${CONTEXT} -n ${NAMESPACE}"

PASS=0
FAIL=0
BOLD=$'\033[1m'; GREEN=$'\033[0;32m'; RED=$'\033[0;31m'; YELLOW=$'\033[0;33m'; RESET=$'\033[0m'

pass()    { printf '%s[smoke] ✓%s  %s\n' "${GREEN}"  "${RESET}" "$*"; (( PASS += 1 )); }
fail()    { printf '%s[smoke] ✗%s  %s\n' "${RED}"    "${RESET}" "$*" >&2; (( FAIL += 1 )); }
warn()    { printf '%s[smoke] ⚠%s  %s\n' "${YELLOW}" "${RESET}" "$*"; }
section() { printf '\n%s── %s%s\n' "${BOLD}" "$*" "${RESET}"; }

# ── Read MySQL root password from K8s secret (no .env dependency) ─────────────
MYSQL_ROOT_PW=$(kubectl --context="${CONTEXT}" -n "${NAMESPACE}" \
    get secret lakehouse-secrets \
    -o jsonpath='{.data.MYSQL_ROOT_PASSWORD}' 2>/dev/null | base64 -d || true)

# ── 1. Pod health (AC-01) ─────────────────────────────────────────────────────
section "AC-01  Pod health"
UNHEALTHY=$(${KC} get pods --no-headers 2>/dev/null \
    | grep -v -E '\s+(Running|Completed)\s+' || true)
if [[ -z "${UNHEALTHY}" ]]; then
    pass "All pods Running or Completed"
else
    fail "Unhealthy pods detected:"
    echo "${UNHEALTHY}" | sed 's/^/         /' >&2
fi

# ── 2. Trino → Polaris → MinIO end-to-end write (EDD §14 Phase 1) ─────────────
section "EDD §14  Trino→Polaris→MinIO write/read"
TRINO_WRITE=$(${KC} exec deployment/airflow-scheduler -- \
    trino --server trino:8080 --catalog iceberg \
    --execute "DROP TABLE IF EXISTS iceberg.bronze.smoke_test;
               CREATE TABLE iceberg.bronze.smoke_test (id BIGINT);
               INSERT INTO iceberg.bronze.smoke_test VALUES (1);
               SELECT count(*) FROM iceberg.bronze.smoke_test;" \
    2>/dev/null | tr -d '[:space:]' || true)

if [[ "${TRINO_WRITE}" == "1" ]]; then
    pass "Trino → Polaris REST Catalog → MinIO write/read OK"
    # Cleanup — failure here is non-fatal
    ${KC} exec deployment/airflow-scheduler -- \
        trino --server trino:8080 \
        --execute "DROP TABLE IF EXISTS iceberg.bronze.smoke_test;" \
        2>/dev/null || true
else
    fail "Trino→Polaris→MinIO chain broken (expected count=1, got '${TRINO_WRITE}')"
fi

# ── 3. Iceberg schema presence (AC-03) ────────────────────────────────────────
section "AC-03  Iceberg schemas (bronze / silver / gold / cache)"
SCHEMAS=$(${KC} exec deployment/airflow-scheduler -- \
    trino --server trino:8080 \
    --execute "SHOW SCHEMAS IN iceberg;" \
    2>/dev/null | tr -d '" ' || true)

for schema in bronze silver gold cache; do
    if echo "${SCHEMAS}" | grep -qx "${schema}"; then
        pass "iceberg.${schema} exists"
    else
        fail "iceberg.${schema} NOT found — Polaris bootstrap may have failed"
    fi
done

# ── 4. data-source API (AC-04) ────────────────────────────────────────────────
section "AC-04  data-source HTTP API"
HTTP_OK=$(${KC} exec deployment/data-source -- \
    python3 -c "
import urllib.request, sys
try:
    r = urllib.request.urlopen('http://localhost:8080/api/tickets', timeout=10)
    data = r.read()
    print('ok' if r.getcode() == 200 and len(data) > 2 else 'fail')
except Exception as e:
    print('fail:' + str(e))
" 2>/dev/null || true)

if [[ "${HTTP_OK}" == "ok" ]]; then
    pass "GET /api/tickets → HTTP 200 + JSON body"
else
    fail "data-source /api/tickets failed (${HTTP_OK})"
fi

# ── 5. Airflow DAGs active (AC-06) ────────────────────────────────────────────
section "AC-06  Airflow DAG state"
DAG_LIST=$(${KC} exec deployment/airflow-scheduler -- \
    airflow dags list 2>/dev/null || true)

for dag in lakehouse_streaming lakehouse_daily; do
    if echo "${DAG_LIST}" | grep -q "${dag}"; then
        pass "DAG ${dag} registered"
    else
        fail "DAG ${dag} not found — DAG discovery may not have completed"
    fi
done

# ── 6. MySQL hot tier tables (prerequisite for AC-15) ─────────────────────────
section "MySQL  hot tier tables"
if [[ -z "${MYSQL_ROOT_PW}" ]]; then
    warn "Could not read MYSQL_ROOT_PASSWORD from secret — skipping MySQL check"
else
    MYSQL_TABLES=$(${KC} exec deployment/mysql -- \
        mysql -u root -p"${MYSQL_ROOT_PW}" -N \
        -e "SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'lakehouse_cache'
              AND table_name IN ('cache_ticket_daily','cache_ticket_hourly');" \
        2>/dev/null | tr -d '[:space:]' || true)

    if [[ "${MYSQL_TABLES}" == "2" ]]; then
        pass "MySQL cache_ticket_daily + cache_ticket_hourly exist"
    else
        fail "MySQL lakehouse_cache tables missing (found ${MYSQL_TABLES}/2)"
    fi
fi

# ── Summary ───────────────────────────────────────────────────────────────────
printf '\n%s══ Smoke test complete: %d passed, %d failed%s\n' \
    "${BOLD}" "${PASS}" "${FAIL}" "${RESET}"

if (( FAIL > 0 )); then
    printf '%sFAIL%s — fix the issues above before using this deployment.\n' \
        "${RED}" "${RESET}" >&2
    exit 1
fi

printf '%sPASS%s — deployment verified.\n' "${GREEN}" "${RESET}"
