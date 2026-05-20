# Local Data Lakehouse
# 本地資料湖倉

> An EDD-compliant, one-click Kubernetes demo of a full streaming BI pipeline — synthetic data flows every 15 minutes from a Data Source POD through a Medallion Lakehouse (Bronze→Silver→Gold) to live Metabase dashboards.
>
> EDD 相容的一鍵 Kubernetes 串流 BI pipeline 示範 — 合成資料每 15 分鐘從資料來源 POD 流經 Medallion 架構（Bronze→Silver→Gold）至 Metabase 即時儀表板。

[![Platform](https://img.shields.io/badge/platform-k8s%20k3s-blue)](https://k3s.io)
[![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-v2-blue?logo=apache)](https://iceberg.apache.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://ibalasite.github.io/lakehouse/)

📄 **[Engineering Design Document (EDD)](https://ibalasite.github.io/lakehouse/edd.html)** · **[Quick Reference](https://ibalasite.github.io/lakehouse/)**

---

## Quick Start / 快速開始

**macOS / Linux / WSL2:**
```bash
bash init_env.sh       # generates .env with fresh random credentials
./k8s/deploy.sh        # deploys the full stack (~25 min on first run)
```

**Windows (PowerShell):**
```powershell
.\init_env.ps1         # generates .env with fresh random credentials
.\k8s\deploy.ps1       # deploys the full stack (~25 min on first run)
```

Then open [http://localhost:30300](http://localhost:30300) and log in with the credentials printed by the init script. The **客服問題單日報** dashboard with 5 charts will be ready.

開啟 [http://localhost:30300](http://localhost:30300)，使用 init 腳本列印的密碼登入。**客服問題單日報**儀表板（含 5 張圖表）即可使用。

---

## Table of Contents / 目錄

1. [Project Overview / 專案概覽](#project-overview--專案概覽)
2. [Architecture / 架構](#architecture--架構)
3. [Prerequisites / 前置需求](#prerequisites--前置需求)
4. [Installation / 安裝](#installation--安裝)
5. [Deploy Modes / 部署模式](#deploy-modes--部署模式)
6. [Service URLs / 服務位址](#service-urls--服務位址)
7. [Verifying the Stack / 驗證堆疊](#verifying-the-stack--驗證堆疊)
8. [End-to-End Smoke Test / 端對端冒煙測試](#end-to-end-smoke-test--端對端冒煙測試)
9. [Airflow DAGs](#airflow-dags)
10. [Metabase Dashboard / Metabase 儀表板](#metabase-dashboard--metabase-儀表板)
11. [Credential Management / 憑證管理](#credential-management--憑證管理)
12. [Security Design / 安全設計](#security-design--安全設計)
13. [Environment Variables / 環境變數](#environment-variables--環境變數)
14. [Directory Structure / 目錄結構](#directory-structure--目錄結構)
15. [Troubleshooting / 疑難排解](#troubleshooting--疑難排解)

---

## Project Overview / 專案概覽

This repository is a **self-contained local data lakehouse** that demonstrates a production-representative BI pipeline running entirely on a laptop with Rancher Desktop (k3s). It is intended as a reference implementation and learning environment — not a production deployment.

本專案是一個**自包含的本地資料湖倉**，展示在筆電上以 Rancher Desktop（k3s）執行的生產代表性 BI pipeline。定位為參考實作與學習環境，非生產部署。

**Pipeline layers / Pipeline 分層：**

| Layer | Component | Role |
|---|---|---|
| Data source | Data Source POD | Pure-stdlib Python HTTP server; generates 5–20 synthetic ticket rows every 5 min; exposes `/api/tickets/drain` |
| Object storage | MinIO | S3-compatible store for Iceberg Parquet files |
| Table format | Apache Iceberg v2 | ACID table management, schema evolution, time travel |
| Catalog | Apache Polaris | Iceberg REST catalog with OAuth2 token issuance |
| Transform | dbt (Trino adapter) | Medallion models: Bronze stg → Silver stg → Gold daily + hourly facts + dims |
| Query engine | Trino | Distributed SQL engine; runs dbt models, writes MySQL cache |
| Cache | MySQL 8.0 | Pre-aggregated metrics tables consumed by Metabase |
| Orchestration | Apache Airflow | `lakehouse_streaming` (*/15 min) + `lakehouse_daily` (02:00) + backfill DAGs |
| BI | Metabase | **客服問題單日報** (5 charts) + **即時監控** (3 cards, auto-refresh 15 min) |
| Platform | k3s (Rancher Desktop) | Local Kubernetes with NodePort access |

| 層次 | 元件 | 職責 |
|---|---|---|
| 資料來源 | Data Source POD | 純 stdlib Python HTTP 伺服器；每 5 分鐘產生 5–20 筆合成問題單；提供 `/api/tickets/drain` |
| 物件儲存 | MinIO | 存放 Iceberg Parquet 檔案的 S3 相容存儲 |
| 表格格式 | Apache Iceberg v2 | ACID 表格管理、結構演進、時間旅行 |
| 目錄 | Apache Polaris | 具備 OAuth2 Token 發行的 Iceberg REST Catalog |
| 轉換 | dbt（Trino adapter）| Medallion 模型：Bronze stg → Silver stg → Gold 日/時 fact + dim |
| 查詢引擎 | Trino | 分散式 SQL 引擎；執行 dbt 模型，寫入 MySQL 快取 |
| 快取 | MySQL 8.0 | Metabase 消費的預聚合指標表 |
| 編排 | Apache Airflow | `lakehouse_streaming`（每 15 分）+ `lakehouse_daily`（02:00）+ 回填 DAG |
| BI | Metabase | **客服問題單日報**（5 張圖）+ **即時監控**（3 張卡，15 分自動刷新）|
| 平台 | k3s（Rancher Desktop）| 具備 NodePort 存取的本地 Kubernetes |

**Seed data:** the stack generates ~10M rows of synthetic customer support tickets using `04_generate_tickets.py` (pyiceberg direct write, bypassing Trino INSERT for speed). After the seed, the Data Source POD continuously generates 5–20 rows every 5 minutes; the `lakehouse_streaming` DAG ingests them every 15 minutes and propagates through the full Medallion pipeline.

**種子資料：** 堆疊使用 `04_generate_tickets.py` 生成約 1,000 萬筆合成客服問題單（pyiceberg 直接寫入，繞過 Trino INSERT 以提升速度）。種子完成後，Data Source POD 每 5 分鐘持續生成 5–20 筆資料；`lakehouse_streaming` DAG 每 15 分鐘攝入並推進整個 Medallion pipeline。

---

## Architecture / 架構

```
init_env.sh  →  .env (random credentials)
     │
     └──  ./k8s/deploy.sh
               │
               ├── Secrets from .env → Kubernetes lakehouse-secrets
               ├── configmap-trino.yaml (${VAR} substituted by deploy.sh)
               │
               ├── MinIO  (S3 API :30900, Console :30901)
               ├── Apache Polaris  (ClusterIP :8181 — use kubectl port-forward)
               ├── Trino  (UI :30080)
               ├── MySQL  (ClusterIP :3306 — use kubectl port-forward)
               ├── Airflow  (UI :30888)
               ├── Metabase  (UI :30300)
               └── Data Source POD  (ClusterIP :8080 — internal only)

               Jobs (run in order):
               01-minio-init       → creates lakehouse bucket
               02-polaris-bootstrap → registers catalog + principal
               04-generate-tickets → 04_generate_tickets.py → Iceberg raw.raw_tickets (~10M rows seed)
               05-metabase-setup   → 05_metabase_setup.py → 2 dashboards (日報 + 即時監控)
```

**Streaming data flow (every 15 min) / 串流資料流（每 15 分）：**

```
Data Source POD
  (generates 5–20 rows / 5 min)
    │
    │  GET /api/tickets/drain  (fetch_and_ingest.py via Airflow)
    ▼
Iceberg raw.raw_tickets  [Bronze]
    │  dbt stg_bronze_tickets
    ▼
Iceberg silver.stg_silver_tickets  [Silver]
    │  dbt fact_ticket_day_wide  +  fact_ticket_hour_wide
    ▼
Iceberg gold.*  [Gold]
    │  populate_mysql_cache.py (--hourly-only)
    ▼
MySQL lakehouse_cache.cache_ticket_daily / cache_ticket_hourly
    │
    └──> Metabase  客服問題單日報  (5 charts, daily)
    └──> Metabase  即時監控       (3 cards, auto-refresh 15 min)
```

---

## Prerequisites / 前置需求

### macOS / Linux

| Requirement | Minimum | Notes |
|---|---|---|
| Rancher Desktop | 1.13+ | Provides k3s + container runtime |
| kubectl | 1.28+ | Bundled with Rancher Desktop |
| Python | 3.9+ | For init scripts and smoke test |
| openssl | any | Used by `init_env.sh` for random bytes |
| cryptography (Python) | any | For Fernet key generation |

### Windows (Native — no WSL2 needed)

| Requirement | Minimum | Notes |
|---|---|---|
| Rancher Desktop | 1.13+ | Download from [rancherdesktop.io](https://rancherdesktop.io); includes kubectl |
| Python | 3.9+ | Download from [python.org](https://python.org); add to PATH during install |
| PowerShell | 5.1+ | Built into Windows 10/11; scripts use `.ps1` |

> **PowerShell execution policy:** Run once as Administrator if needed:
> ```powershell
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
> ```

### Windows (WSL2 option)

Install [WSL2](https://learn.microsoft.com/windows/wsl/install) with Ubuntu, then use the same bash scripts as macOS/Linux inside the Ubuntu terminal. Rancher Desktop must also be installed on the Windows host.

WSL2 方式：安裝 WSL2 + Ubuntu，在 Ubuntu 終端機內執行 bash 腳本。Rancher Desktop 裝在 Windows 主機上，kubectl context 自動共享至 WSL2。

---

**Hardware / 硬體：**
- CPU: 4 cores allocated to the Rancher Desktop VM
- RAM: 8 GB allocated to the Rancher Desktop VM (10 GB recommended)
- Disk: 20 GB free (container images + Iceberg data)

**Python packages for smoke test and seed scripts / 冒煙測試與種子腳本所需 Python 套件：**

```bash
# macOS/Linux/WSL2
pip install 'pyiceberg[s3]' pyarrow mysql-connector-python requests numpy

# Windows (PowerShell)
pip install pyiceberg[s3] pyarrow mysql-connector-python requests numpy
```

**Verify Rancher Desktop is ready / 確認 Rancher Desktop 就緒：**

```bash
kubectl config current-context   # should print: rancher-desktop
kubectl get nodes                 # should show a node in Ready state
```

---

## Installation / 安裝

### Step 1: Clone and generate credentials / 步驟 1：Clone 並產生憑證

**macOS / Linux / WSL2:**
```bash
git clone <repo-url> lakehouse
cd lakehouse
bash init_env.sh
```

**Windows (PowerShell):**
```powershell
git clone <repo-url> lakehouse
cd lakehouse
.\init_env.ps1
```

Both scripts generate a completely fresh set of cryptographically random credentials and write them to `.env`. The file is listed in `.gitignore` and is never committed. The scripts print a summary table of login credentials.

兩個腳本都會產生全新加密隨機憑證並寫入 `.env`。`.env` 已列於 `.gitignore`，永遠不會被 commit。腳本會列印各服務的登入摘要。

Expected output / 預期輸出：

```
╔══════════════════════════════════════════════════════╗
║  Service credentials (valid for this deployment)     ║
╠══════════════════════════════════════════════════════╣
║  MinIO Console  http://localhost:30901
║    user: minio_a3f2b1c4
║    pass: <24-char random>
╠══════════════════════════════════════════════════════╣
║  Airflow        http://localhost:30888
║    user: admin
║    pass: <20-char random>
╠══════════════════════════════════════════════════════╣
║  Metabase       http://localhost:30300
║    user: admin@local.com
║    pass: <20-char random>
╚══════════════════════════════════════════════════════╝
```

### Step 2: Deploy the stack / 步驟 2：部署堆疊

**macOS / Linux / WSL2:**
```bash
./k8s/deploy.sh
```

**Windows (PowerShell):**
```powershell
.\k8s\deploy.ps1
```

The script performs these steps in order / 腳本依序執行：

1. Verifies kubectl can reach the `rancher-desktop` context
2. Creates the `lakehouse` namespace
3. Creates `lakehouse-secrets` from `.env` (no secret YAML is committed to the repo)
4. Applies PVCs and ConfigMaps; substitutes `${VAR}` placeholders in `configmap-trino.yaml` using a Python-based envsubst
5. Deploys MinIO, MySQL, PostgreSQL, Apache Polaris
6. Waits for each deployment to become `Available` before proceeding
7. Runs `01-minio-init` job: creates the `lakehouse` bucket
8. Runs `02-polaris-bootstrap` job: registers the Iceberg catalog and `trino-svc` principal
9. Deploys Trino
10. Runs `04-generate-tickets` job: writes synthetic Iceberg data via `bulk_load_parquet.py`
11. Deploys Airflow (webserver + scheduler)
12. Deploys Metabase, then runs `05-metabase-setup` job to create 2 dashboards (日報 + 即時監控)
13. Deploys Data Source POD (`data-source` Deployment + ClusterIP Service)
14. Prints the service URL summary

**Watch deployment progress / 觀察部署進度：**

```bash
kubectl -n lakehouse get pods -w
```

**Check individual job logs / 查看個別 Job 日誌：**

```bash
kubectl -n lakehouse logs job/minio-init -f
kubectl -n lakehouse logs job/polaris-bootstrap -f
kubectl -n lakehouse logs job/generate-tickets -f
kubectl -n lakehouse logs job/metabase-setup -f
```

**Expected final pod state / 最終 Pod 預期狀態：**

```
NAME                          READY   STATUS      RESTARTS
airflow-scheduler-*           1/1     Running     0
airflow-webserver-*           1/1     Running     0
data-source-*                 1/1     Running     0
metabase-*                    1/1     Running     0
minio-*                       1/1     Running     0
mysql-*                       1/1     Running     0
polaris-*                     2/2     Running     0
postgres-*                    1/1     Running     0
trino-*                       1/1     Running     0
generate-tickets-*            0/1     Completed   0
metabase-setup-*              0/1     Completed   0
minio-init-*                  0/1     Completed   0
polaris-bootstrap-*           0/1     Completed   0
```

---

## Deploy Modes / 部署模式

`deploy.sh` supports three operating modes. Run `bash init_env.sh` before any of them to ensure `.env` is current.

`deploy.sh` 支援三種操作模式。執行任一模式前，先執行 `bash init_env.sh` 確保 `.env` 是最新的。

### Default restart / 預設重啟

```bash
bash init_env.sh
./k8s/deploy.sh
```

Reads `.env`, applies Kubernetes secrets, and redeploys all components. Passwords change on every deploy. **PVC data is preserved** — existing Iceberg files and MySQL tables remain intact. Use this for routine restarts after a laptop reboot.

讀取 `.env`，套用 Kubernetes Secrets 並重新部署所有元件。每次部署密碼都會變更。**PVC 資料保留**——現有 Iceberg 檔案與 MySQL 表不受影響。適用於筆電重開後的例行重啟。

### Skip seed data / 跳過種子資料

```bash
./k8s/deploy.sh --skip-seed
```

Skips the `04-generate-tickets` job. Use this when Iceberg data already exists in the PVC and you only need to redeploy services.

跳過 `04-generate-tickets` Job。當 PVC 中已有 Iceberg 資料，只需重新部署服務時使用。

### Password rotation (preserve data) / 輪換密碼（保留資料）

Rotate all credentials and restart stateless services. MySQL and MinIO data are preserved.

輪換所有憑證並重啟無狀態服務。MySQL 和 MinIO 資料保留不變。

```bash
./k8s/deploy.sh --rotate
```

Internally this: regenerates `.env`, preserves the MySQL/Postgres root passwords (which are baked into the PVCs), updates the k8s Secret, updates the Trino ConfigMap, runs `ALTER USER` in MySQL to rotate the `lakehouse` user password, then rolling-restarts MinIO, Polaris, Trino, Airflow, and Metabase.

此模式內部執行：重新產生 `.env`，保留 MySQL/Postgres root 密碼（已固化在 PVC 中），更新 k8s Secret，更新 Trino ConfigMap，在 MySQL 中執行 `ALTER USER` 輪換 `lakehouse` 使用者密碼，然後滾動重啟 MinIO、Polaris、Trino、Airflow 和 Metabase。

### Full rebuild / 完全重建

Tear down all deployments, PVCs, and jobs, then redeploy from scratch with the current `.env`. All Iceberg and MySQL data is lost.

拆除所有 Deployment、PVC 和 Job，然後以當前 `.env` 從零開始重新部署。所有 Iceberg 和 MySQL 資料將遺失。

```bash
bash init_env.sh        # optional: generate fresh credentials first
./k8s/deploy.sh --rebuild
```

---

## Service URLs / 服務位址

Services exposed via NodePort are accessible directly on `localhost`. MySQL and Polaris are ClusterIP-only; use `kubectl port-forward` to reach them from the host.

透過 NodePort 暴露的服務可直接在 `localhost` 存取。MySQL 和 Polaris 僅為 ClusterIP；從主機存取需使用 `kubectl port-forward`。

| Service | NodePort URL | Auth |
|---|---|---|
| Metabase | http://localhost:30300 | `admin@local.com` / `METABASE_ADMIN_PASSWORD` from `.env` |
| Airflow | http://localhost:30888 | `admin` / `AIRFLOW_ADMIN_PASSWORD` from `.env` |
| Trino UI | http://localhost:30080 | No login required |
| MinIO Console | http://localhost:30901 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |
| MinIO S3 API | http://localhost:30900 | S3 API endpoint |

**Port-forward for ClusterIP services / ClusterIP 服務的連接埠轉發：**

```bash
kubectl -n lakehouse port-forward svc/mysql   3306:3306 &
kubectl -n lakehouse port-forward svc/polaris 8181:8181 &
```

---

## Verifying the Stack / 驗證堆疊

### Check Iceberg row count via Trino / 透過 Trino 查詢 Iceberg 資料筆數

```bash
kubectl -n lakehouse exec deployment/trino -- \
  trino --execute "SELECT COUNT(*) FROM iceberg.raw.raw_tickets" 2>/dev/null | grep -v WARNING
```

### Check MySQL cache tables / 查詢 MySQL 快取表

```bash
MYSQL_PW=$(grep ^MYSQL_PASSWORD .env | cut -d= -f2)
kubectl -n lakehouse exec deployment/mysql -- \
  mysql -ulakehouse -p"${MYSQL_PW}" \
  -e "SELECT COUNT(*) FROM lakehouse_cache.cache_ticket_daily;"
```

### Re-run a failed job / 重新執行失敗的 Job

```bash
kubectl -n lakehouse delete job metabase-setup --ignore-not-found
kubectl -n lakehouse apply -f k8s/jobs/05-metabase-setup.yaml
kubectl -n lakehouse wait job/metabase-setup --for=condition=complete --timeout=300s
```

---

## End-to-End Smoke Test / 端對端冒煙測試

`e2e_smoke_test.py` validates the entire pipeline from data ingestion to Metabase dashboard query. Run it after a fresh deployment or after any change to confirm everything works end-to-end.

`e2e_smoke_test.py` 從資料攝入到 Metabase 儀表板查詢驗證整個 pipeline。在全新部署後或任何更改後執行，以確認端對端正常運作。

```bash
python3 platform/init/e2e_smoke_test.py
```

The test performs these steps in order / 測試依序執行：

1. **Connectivity check**: verifies all 6 services are reachable (Trino, Airflow, Metabase, MinIO via NodePort; Polaris and MySQL via port-forward, auto-started if needed)
2. **Data generation**: writes 500 synthetic ticket rows for today into `iceberg.raw.raw_tickets` via pyiceberg
3. **Airflow trigger**: calls the Airflow REST API to trigger the `lakehouse_hourly` DAG
4. **DAG poll**: polls until the DAG run reaches `success` or `failed` (default 600s timeout)
5. **MySQL verify**: confirms `cache_ticket_hourly` contains rows for today's date
6. **Metabase verify**: logs in, queries all dashboard card endpoints, confirms rows are returned
7. **Summary**: prints `PASS` or `FAIL` per step; exits non-zero if any step failed

**Options / 選項：**

```bash
python3 platform/init/e2e_smoke_test.py --rows 200       # generate 200 rows instead of 500
python3 platform/init/e2e_smoke_test.py --timeout 900    # allow 900s for Airflow DAG
python3 platform/init/e2e_smoke_test.py --skip-generate  # skip Iceberg write (use existing data)
python3 platform/init/e2e_smoke_test.py --skip-airflow   # run cache refresh locally, skip Airflow
```

**Example output / 範例輸出：**

```
[PASS] Connectivity: All services reachable
[PASS] Data generation: 500 rows in 1.3s
[PASS] Airflow DAG (lakehouse_hourly): final state: success
[PASS] MySQL hourly cache: 42 rows for today
[PASS] Metabase dashboard: 318 total rows across cards

All checks passed.
  Dashboard: http://localhost:30300/dashboard/2
```

---

## Airflow DAGs

Four DAGs are deployed automatically. All DAGs read credentials from Kubernetes secrets injected as environment variables — no credentials appear in DAG source code.

四個 DAG 自動部署。所有 DAG 從以環境變數注入的 Kubernetes Secrets 讀取憑證——DAG 原始碼中不含任何憑證。

| DAG | File | Schedule | Purpose |
|---|---|---|---|
| `lakehouse_streaming` | `pipeline_streaming.py` | `*/15 * * * *` | **Primary streaming DAG**: drain data-source pod → Bronze → Silver → dbt Gold hourly → MySQL hourly cache → Metabase refresh |
| `lakehouse_daily` | `pipeline_daily.py` | `0 2 * * *` (02:00 UTC) | Full daily cache refresh: dbt Gold daily fact → MySQL daily + hourly tables |
| `lakehouse_backfill` | `pipeline_backfill.py` | Manual trigger only | Accepts `start_date` / `end_date` conf for historical backfill |
| `lakehouse_hourly` | `pipeline_hourly.py` | Manual trigger / smoke test | Refreshes today's hourly rows; used by `e2e_smoke_test.py` |

**Trigger a DAG manually / 手動觸發 DAG：**

1. Open http://localhost:30888, log in as `admin`
2. Find the DAG, toggle it **On**
3. Click the **Trigger DAG** button (play icon)

**Trigger via CLI / 透過 CLI 觸發：**

```bash
AIRFLOW_PW=$(grep ^AIRFLOW_ADMIN_PASSWORD .env | cut -d= -f2)
curl -s -X POST http://localhost:30888/api/v1/dags/lakehouse_daily/dagRuns \
  -u "admin:${AIRFLOW_PW}" \
  -H "Content-Type: application/json" \
  -d '{}' | python3 -m json.tool
```

**Trigger backfill with date range / 觸發指定日期範圍的回填：**

```bash
AIRFLOW_PW=$(grep ^AIRFLOW_ADMIN_PASSWORD .env | cut -d= -f2)
curl -s -X POST http://localhost:30888/api/v1/dags/lakehouse_backfill/dagRuns \
  -u "admin:${AIRFLOW_PW}" \
  -H "Content-Type: application/json" \
  -d '{"conf": {"start_date": "2025-01-01", "end_date": "2025-12-31"}}' \
  | python3 -m json.tool
```

---

## Metabase Dashboard / Metabase 儀表板

**URL:** http://localhost:30300  
**Login:** `admin@local.com` / value of `METABASE_ADMIN_PASSWORD` in `.env`

Two dashboards are created automatically by `05_metabase_setup.py`.

`05_metabase_setup.py` 自動建立兩個儀表板。

### 客服問題單日報（Daily Report）

5 charts querying the MySQL `lakehouse_cache` database.

| Chart | Description |
|---|---|
| Daily ticket volume trend | Line chart: new tickets per day over the past 90 days |
| Sub-site distribution | Bar chart: ticket counts grouped by `catsub_id` |
| SLA compliance | Line chart: `pct_within_sla` with threshold reference line |
| Complaint ticket ratio | Trend of `complain_tickets / total_tickets` over time |
| Average response and resolution time | Dual-metric chart: `avg_response_hours` and `avg_resolution_hours` |

### 即時監控（Realtime Dashboard）

Auto-refreshes every 15 minutes (`?refresh=900`). 3 cards fed by the `lakehouse_streaming` DAG:

| Card | Description |
|---|---|
| 今日問題單即時動態 | Bar chart: today's ticket count grouped by hour (updated every 15 min) |
| 各子站即時問題單分布 | Row chart: live sub-site breakdown from `cache_ticket_hourly` |
| 今日每小時資料明細 | Table with `MAX(updated_at)` — lets you visually verify the pipeline wrote within the last 15 min |

即時監控每 15 分鐘自動刷新，3 張卡片由 `lakehouse_streaming` DAG 提供資料。
「今日每小時資料明細」中的「最後寫入時間」欄位可確認 pipeline 是否按時更新。

If the dashboard is empty or charts show no data, re-run the setup job / 若儀表板空白或圖表無資料，重新執行 setup job：

```bash
kubectl -n lakehouse delete job metabase-setup --ignore-not-found
kubectl -n lakehouse apply -f k8s/jobs/05-metabase-setup.yaml
kubectl -n lakehouse logs job/metabase-setup -f
```

---

## Credential Management / 憑證管理

### How credentials are generated / 憑證如何產生

`init_env.sh` uses three sources of randomness / `init_env.sh` 使用三種隨機來源：

- `openssl rand -hex` for usernames and hex tokens
- `/dev/urandom` filtered through `tr` for alphanumeric passwords
- Python `cryptography.fernet.Fernet.generate_key()` for the Airflow Fernet key (with `openssl rand -base64 32` as fallback)

Every execution of `init_env.sh` produces a completely different `.env`. There are no static default passwords anywhere in this codebase.

每次執行 `init_env.sh` 都會產生完全不同的 `.env`。此程式碼庫中沒有任何靜態預設密碼。

### How credentials reach services / 憑證如何到達服務

```
.env
 │
 ├── kubectl create secret generic lakehouse-secrets --from-env-file=.env
 │       │
 │       └── Pod env vars via secretKeyRef in each YAML manifest
 │
 └── deploy.sh envsubst → configmap-trino.yaml (${VAR} replaced at deploy time)
         │
         └── Trino reads catalog config from the substituted ConfigMap
```

No secret values appear in any committed file. Kubernetes YAML files reference `secretKeyRef` only. The Trino ConfigMap template in the repository contains `${VAR}` placeholders with no real values.

任何已 commit 的檔案中都不含密碼值。Kubernetes YAML 檔案只使用 `secretKeyRef` 引用。儲存庫中的 Trino ConfigMap 範本含有 `${VAR}` 佔位符，沒有真實值。

### Python scripts and the `_require_env` pattern / Python 腳本與 `_require_env` 模式

All Python init scripts use a hard-fail pattern for reading credentials. **`os.environ.get("KEY", "default")` is explicitly banned** — if a variable is missing, the script raises an error immediately rather than silently using a fallback that might be stale or insecure.

所有 Python init 腳本使用嚴格失敗模式讀取憑證。**明確禁止 `os.environ.get("KEY", "default")`**——若變數不存在，腳本立即報錯，而非默默使用可能過期或不安全的回退值。

```python
# Correct — raises immediately if var is missing
def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required env var {name!r} is not set. Run: bash init_env.sh")
    return value

MYSQL_PASSWORD = _require_env("MYSQL_PASSWORD")   # correct
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "changeme")  # WRONG — never do this
```

---

## Security Design / 安全設計

### Principle of least privilege / 最小權限原則

**MySQL `lakehouse` user / MySQL `lakehouse` 使用者：**

- `lakehouse_cache` database: `SELECT, INSERT, UPDATE, DELETE` only — no DDL. Schema is managed exclusively by `03_mysql_init.sql` run at init time.
- `metabase` database: `ALL PRIVILEGES` — Metabase requires DDL to manage its own internal schema.

**MinIO / MinIO：**

- Root credentials (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`) are used only for bucket creation during the init job.
- For production use, create per-service access keys with bucket-scoped policies. This is not implemented in the local dev stack but is the recommended next step.

**Apache Polaris / Apache Polaris：**

- The `trino-svc` principal is granted `all_access_role` scoped to the `lakehouse` catalog only.
- Iceberg operations use `PRINCIPAL_ROLE:ALL` scope for the service account token.
- Polaris vended credentials are disabled (`vended-credentials-enabled=false`); Trino accesses MinIO directly with its own credentials.

**Trino / Trino：**

- Reads Iceberg tables via Polaris OAuth2 (catalog-scoped token, issued per request).
- Reads and writes MySQL cache via the `lakehouse` user (DML-only on `lakehouse_cache`).

**Kubernetes / Kubernetes：**

- No `hostPath` volumes — all persistent storage uses PVCs.
- Credentials injected via `secretKeyRef` only — no literal values in any YAML.
- No `--privileged` containers.

### Why hardcoded defaults are dangerous / 為何硬編碼預設值危險

Using `os.environ.get("KEY", "hardcoded_default")` creates two risks / 使用 `os.environ.get("KEY", "hardcoded_default")` 產生兩種風險：

1. **Stale credential reuse**: if the env var is missing (misconfigured deployment), the script silently uses the hardcoded value instead of failing. The hardcoded value may be weak, shared, or already compromised.
2. **Accidental commit risk**: a developer who copies the pattern may commit a file containing a real-looking default that becomes a permanent secret in git history.

The `_require_env` pattern eliminates both risks: if the variable is absent, the process fails loudly with a clear message pointing to `init_env.sh`.

1. **過期憑證重用**：若 env var 不存在（部署設定錯誤），腳本默默使用硬編碼值而非失敗。硬編碼值可能較弱、共享或已被洩露。
2. **意外 commit 風險**：複製此模式的開發者可能 commit 含有看似真實預設值的檔案，這個值永遠留存於 git 歷史中。

`_require_env` 模式消除兩種風險：若變數不存在，程序明確失敗並顯示指向 `init_env.sh` 的清楚訊息。

### No credentials in git history / git 歷史中無憑證

The following guarantees are enforced by design / 以下保證由設計強制執行：

- `.env` is in `.gitignore` and was never committed
- All Kubernetes YAML files use `secretKeyRef` — no literal secret values
- `configmap-trino.yaml` in the repo contains only `${VAR}` placeholders; real values are substituted by `deploy.sh` at apply time and never written to disk
- DAG scripts read env vars injected by Kubernetes; no credentials appear in DAG Python source
- `docker history` contains no credentials because no `ENV` or `ARG` instructions with secret values are used in any Dockerfile

### Production hardening checklist / 生產環境強化清單

This stack is designed for local development. Before promoting to production / 此堆疊設計用於本地開發，晉升至生產前需：

- [ ] Enable TLS on all service endpoints
- [ ] Add Kubernetes NetworkPolicies to restrict inter-pod communication
- [ ] Replace MinIO root credentials with per-service IAM-style access keys
- [ ] Integrate a secrets manager (HashiCorp Vault, AWS Secrets Manager, or equivalent) to eliminate `.env` files entirely
- [ ] Replace Metabase embedded H2 with a production PostgreSQL backend
- [ ] Add Kubernetes RBAC policies scoped to the `lakehouse` namespace
- [ ] Enable Polaris persistent storage (replace in-memory catalog with a database backend)
- [ ] Rotate all credentials before first production run

---

## Environment Variables / 環境變數

All variables are generated by `init_env.sh`. You never need to set them manually.

所有變數由 `init_env.sh` 產生，無需手動設定。

| Variable | Description |
|---|---|
| `MINIO_ROOT_USER` | MinIO root username (format: `minio_<hex>`) |
| `MINIO_ROOT_PASSWORD` | MinIO root password (24-char alphanumeric) |
| `MINIO_BUCKET` | Iceberg bucket name (default: `lakehouse`) |
| `POLARIS_CLIENT_ID` | Polaris OAuth2 client ID (format: `polaris_<hex>`) |
| `POLARIS_CLIENT_SECRET` | Polaris OAuth2 client secret (32-char alphanumeric) |
| `POLARIS_CATALOG` | Polaris catalog name (default: `lakehouse`) |
| `MYSQL_ROOT_PASSWORD` | MySQL root password (24-char alphanumeric) |
| `MYSQL_PASSWORD` | MySQL `lakehouse` user password (24-char alphanumeric) |
| `POSTGRES_PASSWORD` | PostgreSQL password for Airflow metadata DB |
| `AIRFLOW_ADMIN_PASSWORD` | Airflow `admin` user password |
| `AIRFLOW__CORE__FERNET_KEY` | Fernet key for Airflow connection encryption |
| `METABASE_ADMIN_PASSWORD` | Metabase `admin@local.com` password |

---

## Directory Structure / 目錄結構

```
lakehouse/
├── init_env.sh                          # Step 1: generate .env (macOS/Linux)
├── init_env.ps1                         # Step 1: generate .env (Windows PowerShell)
├── k8s/
│   ├── deploy.sh                        # Step 2: one-click deploy (macOS/Linux)
│   ├── show_services.sh                 # Print all service URLs + current credentials
│   ├── deploy.ps1                       # Step 2: one-click deploy (Windows PowerShell)
│   ├── data-source.yaml                 # Data Source POD: Deployment + ClusterIP Service
│   ├── namespace.yaml / pvc.yaml
│   ├── minio.yaml / mysql.yaml / postgres.yaml
│   ├── polaris.yaml / trino.yaml
│   ├── airflow.yaml / metabase.yaml
│   ├── configmap-mysql-init.yaml        # MySQL schema DDL (no credentials)
│   ├── configmap-trino.yaml             # Trino catalog config with ${VAR} placeholders
│   ├── configmap-polaris.yaml / configmap-scope-proxy.yaml
│   └── jobs/
│       ├── 01-minio-init.yaml           # Creates lakehouse bucket in MinIO
│       ├── 02-polaris-bootstrap.yaml    # Registers Iceberg catalog and principal
│       ├── 04-generate-tickets.yaml     # Runs 04_generate_tickets.py (seed ~10M rows)
│       └── 05-metabase-setup.yaml       # Creates 2 dashboards (日報 + 即時監控)
├── platform/
│   ├── data_source/
│   │   └── app.py                       # Data Source POD: pure-stdlib HTTP server
│   ├── airflow/
│   │   ├── requirements.txt             # pyiceberg[s3], boto3, pyarrow
│   │   └── dags/
│   │       ├── pipeline_streaming.py    # lakehouse_streaming DAG (*/15 min) ← PRIMARY
│   │       ├── pipeline_daily.py        # lakehouse_daily DAG (02:00 UTC daily)
│   │       ├── pipeline_backfill.py     # lakehouse_backfill DAG (manual trigger)
│   │       └── pipeline_hourly.py       # lakehouse_hourly DAG (smoke test / manual)
│   └── init/
│       ├── fetch_and_ingest.py          # Drain data-source pod → Iceberg via pyiceberg
│       ├── bulk_load_parquet.py         # Fast seed writer (~200k–1M rows/sec)
│       ├── populate_mysql_cache.py      # Trino → MySQL aggregation (executemany batches)
│       ├── 05_metabase_setup.py         # Metabase 2-dashboard auto-setup via REST API
│       └── e2e_smoke_test.py            # End-to-end pipeline verification (6-step)
├── dbt/
│   └── models/
│       ├── bronze/                      # stg_bronze_tickets (append-only raw ingest)
│       ├── silver/                      # stg_silver_tickets (PII mask, SLA flags)
│       ├── gold/macros/                 # generate_schema_name (prevents raw_gold prefix)
│       └── gold/
│           ├── facts/
│           │   ├── fact_ticket_day_wide.sql   # daily aggregation (MERGE, incremental)
│           │   └── fact_ticket_hour_wide.sql  # hourly aggregation (MERGE, trailing 24h)
│           └── dims/                         # dim_date, dim_catsub, dim_perform, …
└── contracts/
    └── metrics/
        ├── field_registry.yml           # EDD metric contract: 12 ticket KPI fields
        └── namespace_registry.yml       # EDD namespace registry → iceberg.raw.raw_tickets
```

**Key file notes / 關鍵檔案說明：**

- `platform/data_source/app.py` is a pure-stdlib Python HTTP server with `enableServiceLinks: false` in its pod spec (prevents k8s from injecting `DATA_SOURCE_PORT=tcp://...` which would crash `int()` parsing). The pod generates 5–20 rows every 5 minutes into an in-memory deque; `GET /api/tickets/drain` atomically returns and clears the buffer.
- `platform/init/fetch_and_ingest.py` polls `/api/tickets/drain`, converts ISO timestamps to microsecond epoch for PyArrow, and appends to `iceberg.raw.raw_tickets` via pyiceberg direct write. Called by the `lakehouse_streaming` DAG every 15 minutes.
- `dbt/models/gold/facts/fact_ticket_hour_wide.sql` is an incremental MERGE model with a 7-dimension composite unique key; watermark covers the trailing 24 hours to absorb late silver updates.
- `04_generate_tickets.py` writes 10M rows directly to Iceberg via pyiceberg and MinIO, bypassing Trino INSERT. This achieves approximately 200,000–1,000,000 rows per second on typical laptop hardware.
- `populate_mysql_cache.py` runs aggregation SQL on Trino and bulk-inserts into MySQL using `executemany` with 5,000-row batches. Supports `--hourly-only` flag for the streaming DAG to skip daily table updates.
- `configmap-trino.yaml` is the only YAML that requires envsubst. It contains `${POLARIS_CLIENT_ID}`, `${POLARIS_CLIENT_SECRET}`, `${MINIO_ROOT_USER}`, and `${MINIO_ROOT_PASSWORD}` placeholders that `deploy.sh` substitutes from `.env` using an inline Python script before applying.

- `platform/data_source/app.py` 是純 stdlib Python HTTP 伺服器，pod spec 設定 `enableServiceLinks: false`（防止 k8s 注入 `DATA_SOURCE_PORT=tcp://...` 造成 `int()` 解析崩潰）。pod 每 5 分鐘向記憶體 deque 生成 5–20 筆資料；`GET /api/tickets/drain` 原子性地返回並清除緩衝區。
- `platform/init/fetch_and_ingest.py` 輪詢 `/api/tickets/drain`，將 ISO 時間戳轉換為 PyArrow 所需的微秒 epoch，並透過 pyiceberg 直接寫入追加至 `iceberg.raw.raw_tickets`。由 `lakehouse_streaming` DAG 每 15 分鐘呼叫。
- `dbt/models/gold/facts/fact_ticket_hour_wide.sql` 是增量 MERGE 模型，具備 7 維複合 unique_key；水位線涵蓋過去 24 小時以吸收遲到的 silver 更新。
- `04_generate_tickets.py` 寫入 1,000 萬筆資料，透過 pyiceberg 和 MinIO 直接寫入 Iceberg，在典型筆電硬體上可達約 200,000–1,000,000 行/秒。
- `populate_mysql_cache.py` 在 Trino 執行聚合 SQL，以 5,000 行批次透過 `executemany` 大量插入 MySQL。支援 `--hourly-only` 旗標供串流 DAG 跳過日報表更新。
- `configmap-trino.yaml` 是唯一需要 envsubst 的 YAML，`deploy.sh` 在套用前使用內嵌 Python 腳本從 `.env` 替換佔位符。

---

## Troubleshooting / 疑難排解

### Data Source POD CrashLoopBackOff

If the `data-source` pod crashes with `ValueError: invalid literal for int() with base 10: 'tcp://...'`, it means Kubernetes injected a `DATA_SOURCE_PORT=tcp://ClusterIP:8080` env var that overrides the app's own port variable. This was fixed by adding `enableServiceLinks: false` to the pod spec in `k8s/data-source.yaml`. Re-running `./k8s/deploy.sh --skip-seed` will apply the fix.

若 `data-source` pod 出現 `ValueError: invalid literal for int() with base 10: 'tcp://...'` 崩潰，表示 k8s 注入了 `DATA_SOURCE_PORT=tcp://...` env var 覆蓋了應用程式自己的連接埠變數。此問題已透過在 `k8s/data-source.yaml` 的 pod spec 加入 `enableServiceLinks: false` 修正。重新執行 `./k8s/deploy.sh --skip-seed` 即可套用修正。

### Trino pod OOMKilled / Trino Pod OOMKilled

Increase the Rancher Desktop VM memory allocation to at least 8 GB (10 GB recommended) in **Preferences → Virtual Machine → Memory**.

在 **Preferences → Virtual Machine → Memory** 中將 Rancher Desktop VM 記憶體增加至至少 8 GB（建議 10 GB）。

### Polaris catalog not found after restart / 重啟後 Polaris Catalog 不見

Polaris uses in-memory persistence by default. After a Polaris pod restart, re-run the bootstrap job / Polaris 預設使用記憶體持久化，Pod 重啟後重新執行 bootstrap job：

```bash
kubectl -n lakehouse delete job polaris-bootstrap --ignore-not-found
kubectl -n lakehouse apply -f k8s/jobs/02-polaris-bootstrap.yaml
kubectl -n lakehouse wait job/polaris-bootstrap --for=condition=complete --timeout=180s
```

### Trino cannot connect to MinIO / Trino 無法連接 MinIO

Trino uses the MinIO ClusterIP (not the hostname `minio`) to avoid Netty DNS resolution failures caused by the k3s `ndots:5` search domain. If you modify `configmap-trino.yaml`, ensure the MinIO endpoint uses the ClusterIP or the Kubernetes service DNS short name `minio.lakehouse.svc.cluster.local`.

Trino 使用 MinIO ClusterIP（而非主機名稱 `minio`），以避免 k3s `ndots:5` 搜尋域導致的 Netty DNS 解析失敗。若修改 `configmap-trino.yaml`，確保 MinIO endpoint 使用 ClusterIP 或 Kubernetes 服務完整 DNS 名稱。

### configmap-trino.yaml applied with literal `${VAR}` / configmap-trino.yaml 含原始 `${VAR}` 被套用

This means `kubectl apply -f k8s/configmap-trino.yaml` was run directly instead of through `deploy.sh`. Always apply the Trino ConfigMap via `deploy.sh`, which performs the envsubst substitution first.

這表示直接執行了 `kubectl apply -f k8s/configmap-trino.yaml` 而非透過 `deploy.sh`。Trino ConfigMap 必須透過 `deploy.sh` 套用，它會先執行 envsubst 替換。

### Metabase dashboard shows no data / Metabase 儀表板無資料

Verify the MySQL cache tables have been populated, then re-run the setup job / 確認 MySQL 快取表已填充，然後重新執行 setup job：

```bash
MYSQL_PW=$(grep ^MYSQL_PASSWORD .env | cut -d= -f2)
kubectl -n lakehouse exec deployment/mysql -- \
  mysql -ulakehouse -p"${MYSQL_PW}" \
  -e "SELECT COUNT(*), MAX(date_sk) FROM lakehouse_cache.cache_ticket_daily;"

kubectl -n lakehouse delete job metabase-setup --ignore-not-found
kubectl -n lakehouse apply -f k8s/jobs/05-metabase-setup.yaml
```

### e2e_smoke_test.py fails at Airflow step / 冒煙測試在 Airflow 步驟失敗

Ensure the Airflow scheduler has loaded the DAGs. If the scheduler was recently restarted, wait 30 seconds for DAG discovery / 確認 Airflow Scheduler 已載入 DAG。若 Scheduler 最近重啟，等待 30 秒讓 DAG 被發現：

```bash
kubectl -n lakehouse rollout restart deployment/airflow-scheduler
# wait ~30 seconds, then retry
python3 platform/init/e2e_smoke_test.py --skip-airflow   # local fallback
```

---

## License / 授權

MIT

**Note on data:** Synthetic ticket data generated by `bulk_load_parquet.py` and `04_generate_tickets.py` is randomly generated using NumPy. It mimics the schema of real customer support tickets but contains no real user data.

**資料說明：** 由 `bulk_load_parquet.py` 與 `04_generate_tickets.py` 產生的合成問題單資料使用 NumPy 隨機生成，模仿真實客服問題單結構，但不含任何真實使用者資料。
