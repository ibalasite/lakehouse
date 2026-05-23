# Lakehouse — Agent 操作指南

> **Source of truth**：先查 `docs/edd.md`，再查官方文件，不猜測，不試錯。
> CLAUDE.md 只放行為約束與關鍵 gotcha，詳細規格在 EDD 對應章節。

---

## 絕對約束（最高優先，不可違反）

**只有為了更符合 EDD 目標、加強規格、擴大範圍，才能主動調整實作。**

- 嚴禁以「資料量小」「本地環境」等理由縮小範圍或降低規格
- EDD 說 MERGE → 實作 MERGE；EDD 說日分區 → 760 個日分區；EDD 說要某欄位 → 加上去
- 任何縮小範圍或跳過執行的建議，必須先取得明確書面授權

---

## 常用指令

```bash
# 從零完整重建整個 lakehouse
./k8s/deploy.sh --rebuild

# 跳過 seed 資料的快速重建（開發迭代用）
./k8s/deploy.sh --rebuild --skip-seed

# 執行單一 dbt model（寫入 Iceberg）
cd dbt && dbt run --select <model> --profiles-dir . --target prod

# 執行同一 model 寫入 MySQL hot tier
cd dbt && dbt run --select <model> --profiles-dir . --target mysql_cache

# 在叢集內執行 Trino 查詢
trino --server trino:8080 --catalog iceberg --schema gold --execute "SELECT ..."

# 查看 Airflow task logs
kubectl -n lakehouse logs -f deployment/airflow --tail=100

# 列出所有服務 URL + 帳密
./k8s/show_services.sh
```

---

## 架構

```
MinIO (S3) → Polaris REST Catalog → Trino → dbt-trino → Airflow → Metabase
                    ↓
               PostgreSQL（Polaris + Airflow metadata 持久化）
```

**Medallion 層次**（完整規格見 `docs/edd.md §8`）：

```
raw_tickets → bronze（append）→ silver（merge）
  → gold dims / hour_long / hour_wide / day_wide / month_wide
  → cache（雙 target：iceberg + mysql）→ MySQL hot tier
```

**DAG 排程**：
- `lakehouse_streaming`：`*/15 * * * *` — ingest → bronze → silver → hour_long → hour_wide → cache_daily_report（iceberg + mysql）
- `lakehouse_daily`：`0 2 * * *` — 完整 medallion + dbt test + MySQL partition rotation

---

## 關鍵 Gotcha（忽略必壞）

### 1. Polaris 必須連 PostgreSQL（否則 pod restart 清空所有 catalog）

```
POLARIS_PERSISTENCE_TYPE=relational-jdbc
QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://postgres:5432/<db>
QUARKUS_DATASOURCE_USERNAME=<pg-user>
QUARKUS_DATASOURCE_PASSWORD=<pg-password>
```

首次啟動前必須執行 `polaris-admin-tool bootstrap`。詳見 `docs/edd.md §3.2`。

### 2. Trino ConfigMap 必須先 envsubst 再 apply

`k8s/configmap-trino.yaml` 含 `${VAR}` 佔位符。直接 `kubectl apply` 會把字面 `${...}` 注入 Trino config，破壞 OAuth2 與 catalog 連線。一律透過 `deploy.sh` 執行，它會先跑 `envsubst`。

### 3. 靜態 Iceberg pruning window 是必要的

Subquery watermark（correlated scalar）不會被 Iceberg 推送到 file-level stats。沒有 `date_add('hour', -N, current_timestamp)` 靜態窗口，每次 incremental run 都是全表掃描。  
streaming 用 `-6h`，daily 用 `--vars '{"bronze_lookback_hours": 48}'`。

### 4. dbt test 範圍：只跑 gold + cache

`dbt test --select bronze silver` 會對 10M 行 Iceberg 做 NOT NULL 全表掃描 → Trino OOMKill。daily DAG 只執行 `dbt test --select gold cache`。

### 5. Trino Xmx = 2500m，不得更改

`4000m` 在 JVM + Metabase 並發下 OOMKill。`2500m` 是驗證過的穩定值。

### 6. 雙 target cache：每個 model 跑兩次

EDD §13.2b：同一 dbt model 分別寫入 Iceberg（`--target prod`）和 MySQL（`--target mysql_cache`）。`generate_schema_name` macro 依 `target.name` 路由。

---

## 禁止事項

| # | 禁止 | 原因 |
|---|------|------|
| 1 | 在 Polaris UI 手動建正式表作為 source of truth | Git contracts + dbt 才是 source of truth |
| 2 | dbt SQL 內 hardcode `iceberg.gold.*` | 一律用 `{{ ref() }}` |
| 3 | dbt model SQL 裡出現 `target.type` | 差異邏輯只能放 `macros/platform/*` |
| 4 | Gold serving fact 出現 TIMESTAMP 或 VARCHAR | EDD §9.1：跨引擎型別安全 |
| 5 | Bronze 做 UPDATE / DELETE | Append-only；GDPR purge 有專屬 runbook |
| 6 | BI 查詢寫非 `cache.*` 或 MySQL hot tier 的 SQL | BI 只看 pre-joined cache 層 |
| 7 | 密碼寫入 YAML、Python 源碼或 K8s manifest | 全部透過 `.env` → K8s `secretKeyRef` |

---

## Gold Fact 型別規範（EDD §9）

| 欄位類型 | 規則 |
|---------|------|
| 日期 | `DATE` — 嚴禁 TIMESTAMP（時區問題） |
| 小時 | `INT 0–23` — 嚴禁 `time_sk BIGINT HHMMSS` |
| 維度 SK | `BIGINT`，用 `stable_hash64_number()` macro |
| 量值 | `DECIMAL(38,12)` 或 `DOUBLE` |
| VARCHAR | **禁止** — 改用 dim 表 + numeric FK |

`stable_hash64_number` 的 NULL sentinel = `'__NULL__'`（大寫，EDD §9.4）。

---

## 水位線策略（EDD §8.6）

| 層 | 水位線欄位 | lookback |
|----|-----------|---------|
| bronze → silver | `ingested_at` | `MAX(ingested_at) - 1 min FROM this` |
| silver → hour_long | `ingested_at` | `MAX(updated_at) - 1 min FROM this` |
| hour_long → hour_wide | `updated_at` | `MAX(updated_at) FROM this` |
| hour_wide → day_wide | `prblm_date` | 7 天滾動 |
| day_wide → month_wide | `prblm_date` | 2 個月滾動 |

---

## MySQL 分區策略（EDD §10.6.5）

- **760 個日分區** = 730 活躍天 + 30 天緩衝，命名格式 `p2024_05_24`
- 永遠保留 `p_future VALUES LESS THAN MAXVALUE`
- daily DAG 的 `mysql_rotate_partitions` task 每天刪過期分區、重組 `p_future` 加入明天
- 嚴禁改回月分區

---

## EDD 快查索引

> 用 `Read docs/edd.md offset=<行號> limit=80` 直接跳到目標段落，不要整份展開。

### 改寫 dbt SQL 時

| 需要查什麼 | 章節 | 起始行 |
|-----------|------|--------|
| Bronze schema、metadata 欄位規範 | §8.2 | 684 |
| Silver MERGE 策略、dedup 邏輯 | §8.3 | 741 |
| Gold 型別規範（DATE/INT/BIGINT/DECIMAL）| §9.1 | 875 |
| PII 處理（mask_name / pii_hash）| §9.3 | 1005 |
| `stable_hash64_number` 實作與 sentinel | §9.4 | 1108 |
| `fact_ticket_hour_long` EAV 結構 | §9.5 | 1217 |
| `fact_ticket_hour_wide` PIVOT 邏輯 | §9.6 | 1290 |
| `fact_ticket_day_wide` 7-day MERGE | §9.7 | 1397 |
| `fact_ticket_month_wide` 2-month MERGE | §9.8 | 1437 |
| Lambda UNION ALL cache（cache_daily_report）| §9.10 | 1457 |
| MySQL hot tier DDL 與分區策略 | §10.6 | 1696 |
| dbt macro 命名規範 | §12.1 | 2182 |
| dbt profiles（prod / mysql_cache target）| §12.2 | 2206 |

### 修改 DAG / pipeline 時

| 需要查什麼 | 章節 | 起始行 |
|-----------|------|--------|
| 統一水位線策略（各層水位線欄位）| §8.6 | 771 |
| DAG 列表與排程定義 | §13.1 | 2330 |
| Streaming / Daily DAG task 順序 | §13.2 | 2344 |
| 雙 target cache 執行方式 | §13.2b | 2362 |

### 排查 / 運維時

| 需要查什麼 | 章節 | 起始行 |
|-----------|------|--------|
| Local 啟動步驟 | §16.1 | 2792 |
| dbt build 流程 | §16.2 | 2817 |
| 新增 metric field 完整流程 | §16.4 | 2854 |
| Backfill hour/day/month | §16.5 | 2879 |
| Cache stale 排查 | §16.7 | 2931 |
| dbt tests 規範（各層）| §17 | 3028 |

### 看完整範例時

| 需要查什麼 | 章節 | 起始行 |
|-----------|------|--------|
| Bronze → MV 完整實戰走一遍 | §29 Appendix E | 3552 |
| Bronze 範例 SQL | §29 E.3 | 3603 |
| Silver 範例 SQL | §29 E.4 | 3653 |
| Gold hour_long 範例 SQL | §29 E.6 | 3847 |
| Gold hour_wide 範例 SQL | §29 E.7 | 3959 |
| Cache UNION ALL 範例 SQL | §29 E.12 | 4225 |
| 資料流 row-by-row 追蹤 | §25 Appendix B | 3312 |

---

## 決策流程

1. 查上面索引 → `Read docs/edd.md offset=<行號> limit=80`（需要更多就增加 limit）
2. Web search 官方文件（Apache Polaris / Trino / dbt / Airflow）
3. 確認做法後再動手

---

## 驗收標準與專案範圍

詳見 `docs/edd.md §14`（AC-01 至 AC-19）及六維度上線標準（P1–P6）。

**不在範圍**：AWS/雲端生產環境、Trino worker 水平自動擴展、Metabase 使用者權限管理、Iceberg snapshot 自動清理。
