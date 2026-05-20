#!/usr/bin/env pwsh
# k8s/deploy.ps1 — Deploy the lakehouse to Rancher Desktop (k3s) on Windows
#
# Usage:
#   cd C:\path\to\lakehouse
#   .\k8s\deploy.ps1 [--skip-seed] [--skip-jobs] [--rotate] [--rebuild]
#
#   --rotate   Regenerate credentials, update secrets, rolling-restart services.
#              MySQL/MinIO data is preserved. DB root passwords are not rotated.
#   --rebuild  Delete all deployments/PVCs and redeploy from scratch (new data).

$ErrorActionPreference = 'Continue'   # kubectl non-zero exits are handled manually

$ScriptDir  = $PSScriptRoot
$ProjectDir = Split-Path $ScriptDir -Parent
$InitDir    = Join-Path $ProjectDir 'platform\init'
$DagDir     = Join-Path $ProjectDir 'platform\airflow\dags'

$Namespace = 'lakehouse'
$Context   = 'rancher-desktop'

# ── Parse arguments ────────────────────────────────────────────────────────────
$SkipSeed = $false
$SkipJobs = $false
$Rotate   = $false
$Rebuild  = $false

foreach ($arg in $args) {
    switch ($arg) {
        '--skip-seed' { $SkipSeed = $true }
        '--skip-jobs' { $SkipJobs = $true }
        '--rotate'    { $Rotate   = $true }
        '--rebuild'   { $Rebuild  = $true }
        default       { Write-Error "Unknown argument: $arg"; exit 1 }
    }
}

# ── Logging helpers ────────────────────────────────────────────────────────────
$script:StepNum = 0
function Log($msg)     { Write-Host "[deploy] $msg" }
function Success($msg) { Write-Host "[deploy] OK $msg" -ForegroundColor Green }
function Warn($msg)    { Write-Host "[deploy] !! $msg" -ForegroundColor Yellow }
function Fail($msg)    { Write-Host "[deploy] ERR $msg" -ForegroundColor Red; exit 1 }
function Step($msg) {
    $script:StepNum++
    Write-Host ""
    Write-Host "== Step $($script:StepNum): $msg" -ForegroundColor Cyan
}

# Shorthand: kubectl targeting our namespace
function KC { kubectl --context=$Context -n $Namespace @args }

# ── Envsubst: replace ${VAR} in a YAML template using values from .env ─────────
function Get-Envsubst($templatePath) {
    $envVars = @{}
    Get-Content (Join-Path $ProjectDir '.env') | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith('#') -and $line.Contains('=')) {
            $idx = $line.IndexOf('=')
            $k = $line.Substring(0, $idx).Trim()
            $v = $line.Substring($idx + 1).Trim()
            $envVars[$k] = $v
        }
    }
    $content = Get-Content $templatePath -Raw
    return [regex]::Replace($content, '\$\{([^}]+)\}', {
        param($m)
        $key = $m.Groups[1].Value
        if ($envVars.ContainsKey($key)) { $envVars[$key] } else { '' }
    })
}

function Apply-Envsubst($templatePath) {
    Get-Envsubst $templatePath | kubectl --context=$Context apply -f -
    if ($LASTEXITCODE -ne 0) { Fail "kubectl apply failed for $templatePath" }
}

# ── Wait helpers ───────────────────────────────────────────────────────────────
function Wait-Ready($deploy, [int]$timeout = 180) {
    Log "  Waiting for $deploy (up to ${timeout}s)..."
    kubectl --context=$Context -n $Namespace `
        wait deployment/$deploy --for=condition=Available --timeout="${timeout}s"
    if ($LASTEXITCODE -ne 0) { Fail "$deploy did not become ready within ${timeout}s" }
    Success "$deploy is ready"
}

function Wait-Job($jobName, [int]$timeout = 120) {
    KC wait job/$jobName --for=condition=complete --timeout="${timeout}s"
    if ($LASTEXITCODE -ne 0) {
        KC logs job/$jobName 2>$null
        Fail "$jobName job failed or timed out"
    }
}

# ── Step 1: Prerequisites ──────────────────────────────────────────────────────
Step "Checking prerequisites"

if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) { Fail "kubectl not found — install Rancher Desktop" }
if (-not (Get-Command python3 -ErrorAction SilentlyContinue) -and
    -not (Get-Command python  -ErrorAction SilentlyContinue)) {
    Fail "python not found — install Python 3.9+ from python.org"
}
if (-not (Test-Path (Join-Path $ProjectDir '.env'))) {
    Fail ".env not found — run: .\init_env.ps1"
}

kubectl --context=$Context cluster-info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { Fail "Cannot reach Rancher Desktop cluster (context: $Context)" }
Success "kubectl connected to $Context"
Success ".env ready"

# ── --rebuild: tear down existing resources before fresh deploy ────────────────
if ($Rebuild) {
    Step "Full rebuild (--rebuild): removing existing cluster resources"
    Warn "Deleting all deployments, jobs, PVCs, and ConfigMaps in $Namespace..."
    KC delete deployments --all --ignore-not-found
    KC delete jobs        --all --ignore-not-found
    KC delete pvc         --all --ignore-not-found
    KC delete configmaps  --all --ignore-not-found
    Log "  Waiting for pods to terminate (up to 120s)..."
    KC wait pods --all --for=delete --timeout=120s 2>$null
    Success "Resources deleted — continuing with fresh deploy"
}

# ── --rotate: in-place credential rotation without destroying data ─────────────
if ($Rotate) {
    Step "Credential rotation (--rotate)"

    $envPath      = Join-Path $ProjectDir '.env'
    $oldMysqlRoot = (Get-Content $envPath | Where-Object { $_ -match '^MYSQL_ROOT_PASSWORD=' }) `
                    -replace 'MYSQL_ROOT_PASSWORD=', ''
    $oldPostgresPw = (Get-Content $envPath | Where-Object { $_ -match '^POSTGRES_PASSWORD=' }) `
                    -replace 'POSTGRES_PASSWORD=', ''

    Log "  Regenerating credentials..."
    & (Join-Path $ProjectDir 'init_env.ps1')

    $newMysqlPw = (Get-Content $envPath | Where-Object { $_ -match '^MYSQL_PASSWORD=' }) `
                  -replace 'MYSQL_PASSWORD=', ''

    # Restore DB root passwords (baked into PVCs, can't change via k8s secret alone)
    $pyScript = @'
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
'@
    $tmpPy = [System.IO.Path]::GetTempFileName() + '.py'
    [System.IO.File]::WriteAllText($tmpPy, $pyScript)
    $pyCmd = if (Get-Command python3 -ErrorAction SilentlyContinue) { 'python3' } else { 'python' }
    & $pyCmd $tmpPy $envPath $oldMysqlRoot $oldPostgresPw
    Remove-Item $tmpPy
    Success "DB root passwords preserved; application credentials refreshed"

    Log "  Updating lakehouse-secrets..."
    kubectl --context=$Context -n $Namespace create secret generic lakehouse-secrets `
        "--from-env-file=$envPath" --dry-run=client -o yaml | kubectl --context=$Context apply -f -

    Log "  Updating Trino ConfigMap (Polaris OAuth credentials)..."
    Apply-Envsubst (Join-Path $ScriptDir 'configmap-trino.yaml')

    Log "  Rotating MySQL lakehouse user password via ALTER USER..."
    KC exec deployment/mysql -- `
        mysql -u root "-p$oldMysqlRoot" `
        -e "ALTER USER 'lakehouse'@'%' IDENTIFIED BY '$newMysqlPw'; FLUSH PRIVILEGES;" 2>$null
    if ($LASTEXITCODE -eq 0) { Success "MySQL lakehouse password rotated" }
    else { Warn "MySQL ALTER USER failed — password syncs on next restart" }

    Log "  Rolling restart: MinIO, Polaris, Trino, Airflow, Metabase..."
    foreach ($deploy in @('minio','polaris','trino','airflow-scheduler','airflow-webserver','metabase')) {
        KC rollout restart deployment/$deploy 2>$null
        if ($LASTEXITCODE -eq 0) { Log "  -> $deploy restarting" }
        else { Warn "  -> $deploy not found (skip)" }
    }

    Wait-Ready 'polaris'           180
    Wait-Ready 'trino'             300
    Wait-Ready 'airflow-webserver' 180
    Wait-Ready 'metabase'          180

    Success "Credential rotation complete. MySQL and MinIO data preserved."
    Write-Host "`n  New credentials written to .env`n" -ForegroundColor Green
    exit 0
}

# ── Step 2: Namespace ──────────────────────────────────────────────────────────
Step "Creating namespace $Namespace"
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'namespace.yaml')
if ($LASTEXITCODE -ne 0) { Fail "Failed to create namespace" }
Success "Namespace ready"

# ── Step 3: Secrets ────────────────────────────────────────────────────────────
Step "Creating/updating lakehouse-secrets from .env"
$envPath = Join-Path $ProjectDir '.env'
kubectl --context=$Context -n $Namespace create secret generic lakehouse-secrets `
    "--from-env-file=$envPath" --dry-run=client -o yaml | kubectl --context=$Context apply -f -
if ($LASTEXITCODE -ne 0) { Fail "Failed to apply secrets" }
Success "Secrets applied"

# ── Step 4: PVCs ───────────────────────────────────────────────────────────────
Step "Applying PersistentVolumeClaims"
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'pvc.yaml')
Success "PVCs applied"

# ── Step 5: ConfigMaps ─────────────────────────────────────────────────────────
Step "Applying ConfigMaps"

Apply-Envsubst (Join-Path $ScriptDir 'configmap-trino.yaml')
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'configmap-mysql-init.yaml')
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'configmap-polaris.yaml')
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'configmap-scope-proxy.yaml')

Log "  Building init-scripts ConfigMap ..."
KC create configmap init-scripts `
    "--from-file=04_generate_tickets.py=$(Join-Path $InitDir '04_generate_tickets.py')" `
    "--from-file=05_metabase_setup.py=$(Join-Path $InitDir '05_metabase_setup.py')" `
    --dry-run=client -o yaml | kubectl --context=$Context apply -f -

Log "  Building airflow-scripts ConfigMap ..."
KC create configmap airflow-scripts `
    "--from-file=fetch_and_ingest.py=$(Join-Path $InitDir 'fetch_and_ingest.py')" `
    "--from-file=populate_mysql_cache.py=$(Join-Path $InitDir 'populate_mysql_cache.py')" `
    --dry-run=client -o yaml | kubectl --context=$Context apply -f -

Log "  Building data-source-app ConfigMap ..."
KC create configmap data-source-app `
    "--from-file=app.py=$(Join-Path $ProjectDir 'platform\data_source\app.py')" `
    --dry-run=client -o yaml | kubectl --context=$Context apply -f -

Log "  Building polaris-bootstrap-script ConfigMap ..."
KC create configmap polaris-bootstrap-script `
    "--from-file=bootstrap.py=$(Join-Path $InitDir '02_polaris_bootstrap.py')" `
    --dry-run=client -o yaml | kubectl --context=$Context apply -f -

Log "  Building airflow-dags ConfigMap ..."
KC create configmap airflow-dags `
    "--from-file=pipeline_daily.py=$(Join-Path $DagDir 'pipeline_daily.py')" `
    "--from-file=pipeline_backfill.py=$(Join-Path $DagDir 'pipeline_backfill.py')" `
    "--from-file=pipeline_hourly.py=$(Join-Path $DagDir 'pipeline_hourly.py')" `
    "--from-file=pipeline_streaming.py=$(Join-Path $DagDir 'pipeline_streaming.py')" `
    --dry-run=client -o yaml | kubectl --context=$Context apply -f -

Success "ConfigMaps applied"

# ── Step 6: Core data services ─────────────────────────────────────────────────
Step "Deploying core data services (MinIO, MySQL, PostgreSQL)"
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'minio.yaml')
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'mysql.yaml')
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'postgres.yaml')

# ── Step 7: Polaris ────────────────────────────────────────────────────────────
Step "Deploying Apache Polaris"
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'polaris.yaml')

# ── Step 8: Wait for core services ────────────────────────────────────────────
Step "Waiting for core services to become ready"
Wait-Ready 'minio'    120
Wait-Ready 'mysql'    180
Wait-Ready 'postgres'  90

# ── Step 9: MinIO init job ─────────────────────────────────────────────────────
if (-not $SkipJobs) {
    Step "Running MinIO bucket initialisation job"
    KC delete job minio-init --ignore-not-found 2>$null
    kubectl --context=$Context apply -f (Join-Path $ScriptDir 'jobs\01-minio-init.yaml')
    Log "  Waiting for minio-init job..."
    Wait-Job 'minio-init' 120
    Success "MinIO buckets created"
}

# ── Step 10: Polaris bootstrap job ────────────────────────────────────────────
Step "Waiting for Polaris to be ready"
Wait-Ready 'polaris' 300

if (-not $SkipJobs) {
    Step "Running Polaris bootstrap job"
    KC delete job polaris-bootstrap --ignore-not-found 2>$null
    kubectl --context=$Context apply -f (Join-Path $ScriptDir 'jobs\02-polaris-bootstrap.yaml')
    Log "  Waiting for polaris-bootstrap job..."
    Wait-Job 'polaris-bootstrap' 180
    Success "Polaris catalog bootstrapped"
}

# ── Step 11: Trino ─────────────────────────────────────────────────────────────
Step "Deploying Trino"
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'trino.yaml')
Wait-Ready 'trino' 300

# ── Step 12: Data Source pod ───────────────────────────────────────────────────
Step "Deploying data-source pod"
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'data-source.yaml')
Wait-Ready 'data-source' 60
Success "Data source pod ready"

# ── Step 12b: Generate seed data ───────────────────────────────────────────────
if (-not $SkipJobs -and -not $SkipSeed) {
    Step "Generating seed ticket records"
    KC delete job generate-tickets --ignore-not-found 2>$null
    kubectl --context=$Context apply -f (Join-Path $ScriptDir 'jobs\04-generate-tickets.yaml')
    Log "  Waiting for generate-tickets job (up to 60m)..."
    Wait-Job 'generate-tickets' 3600
    Success "Seed data generated"
} elseif ($SkipSeed) {
    Warn "Skipping seed data generation (--skip-seed)"
}

# ── Step 13: Airflow ───────────────────────────────────────────────────────────
Step "Deploying Airflow"
Apply-Envsubst (Join-Path $ScriptDir 'airflow.yaml')
Log "  Waiting for airflow-init job..."
KC wait job/airflow-init --for=condition=complete --timeout=180s
if ($LASTEXITCODE -ne 0) { KC logs job/airflow-init 2>$null; Fail "airflow-init job failed" }
Wait-Ready 'airflow-webserver' 180
Success "Airflow ready"

# ── Step 14: Metabase ──────────────────────────────────────────────────────────
Step "Deploying Metabase"
kubectl --context=$Context apply -f (Join-Path $ScriptDir 'metabase.yaml')
Wait-Ready 'metabase' 300

if (-not $SkipJobs) {
    Step "Configuring Metabase dashboard"
    KC delete job metabase-setup --ignore-not-found 2>$null
    kubectl --context=$Context apply -f (Join-Path $ScriptDir 'jobs\05-metabase-setup.yaml')
    Log "  Waiting for metabase-setup job..."
    Wait-Job 'metabase-setup' 300
    Success "Metabase dashboard configured"
}

# ── Step 15: Summary ───────────────────────────────────────────────────────────
Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  Lakehouse K8s Stack -- Service URLs" -ForegroundColor Green
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host ("  {0,-22} {1}" -f "MinIO API",     "http://localhost:30900")
Write-Host ("  {0,-22} {1}" -f "MinIO Console", "http://localhost:30901")
Write-Host ("  {0,-22} {1}" -f "Trino UI",      "http://localhost:30080")
Write-Host ("  {0,-22} {1}" -f "Airflow UI",    "http://localhost:30888  (admin / AIRFLOW_ADMIN_PASSWORD in .env)")
Write-Host ("  {0,-22} {1}" -f "Metabase",      "http://localhost:30300  (admin@local.com / METABASE_ADMIN_PASSWORD in .env)")
Write-Host ("  {0,-22} {1}" -f "Data Source",   "http://data-source:8080 (cluster-internal — 5-20 tickets/5min)")
Write-Host ""
Write-Host "  All services deployed." -ForegroundColor Green
Write-Host ""
Write-Host "  Watch pod status:"
Write-Host "    kubectl --context=rancher-desktop -n lakehouse get pods -w"
Write-Host ""
