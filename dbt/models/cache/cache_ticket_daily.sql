{{
  config(
    materialized         = 'incremental',
    table_type           = platform_iceberg_table_type(),
    incremental_strategy = platform_merge_strategy(),
    unique_key           = ['prblm_date', 'catsub_id', 'prblm_source_id',
                            'prblm_class_id', 'prblm_perform_id', 'prblm_status_id'],
    on_schema_change     = 'ignore',
    schema               = 'cache'
  )
}}

/*
  cache_ticket_daily
  ──────────────────────────────────────────────────────────────────────────────
  Dual-target Cache MV (batch layer): fact_ticket_day_wide LEFT JOIN all dims.
  Pre-joined flat table — BI (Metabase / Trino) queries need zero JOINs.

  Runs twice per daily DAG (§13.2b):
    dbt run --select cache_ticket_daily --target prod        → iceberg.cache
    dbt run --select cache_ticket_daily --target mysql_cache → mysql.lakehouse_cache

  Incremental MERGE on grain keys (EDD §10.5): only processes last
  watermark_lookback_days (default 3) on incremental runs; full 730-day
  window on first run or --full-refresh.

  Lineage:  fact_ticket_day_wide + dim_* → cache_ticket_daily (both targets)
*/

SELECT
  -- ── Date ────────────────────────────────────────────────────────────────────
  f.prblm_date,

  -- ── Dimension keys (retained for filtering) ─────────────────────────────────
  f.catsub_id,
  f.prblm_source_id,
  f.prblm_class_id,
  f.prblm_perform_id,
  f.prblm_status_id,

  -- ── Dimension labels (pre-joined for BI) ─────────────────────────────────────
  cat.product_name,
  cat.region                          AS catsub_region,
  cat.catsub_code,
  src.source_name_zh,
  src.source_name_en,
  sts.status_name_zh,
  sts.status_name_en,
  perf.perform_name,
  perf.sla_hours,

  -- ── Volume metrics ───────────────────────────────────────────────────────────
  f.total_tickets,
  f.resolved_tickets,
  f.one_shot_resolved,
  f.complain_tickets,
  f.forwarded_tickets,
  f.within_sla_tickets,

  -- ── Duration components (additive — kept for correct cross-period weighted avg)
  f.sum_resolution_hours,
  f.cnt_resolution_hours,
  f.sum_response_hours,
  f.cnt_response_hours,

  -- ── Pre-computed metrics for BI convenience ──────────────────────────────────
  f.avg_resolution_hours,
  f.avg_response_hours,
  f.pct_resolved,
  f.pct_within_sla,
  f.pct_one_shot,

  -- ── Freshness ────────────────────────────────────────────────────────────────
  f.updated_at

FROM {{ ref('fact_ticket_day_wide') }}        AS f
LEFT JOIN {{ ref('dim_catsub') }}             AS cat  ON f.catsub_id       = cat.catsub_id
LEFT JOIN {{ ref('dim_prblm_source') }}       AS src  ON f.prblm_source_id  = src.prblm_source_id
LEFT JOIN {{ ref('dim_prblm_status') }}       AS sts  ON f.prblm_status_id  = sts.prblm_status_id
LEFT JOIN {{ ref('dim_perform') }}            AS perf ON f.prblm_perform_id  = perf.prblm_perform_id

WHERE f.prblm_date >= CURRENT_DATE - INTERVAL '730' DAY
{% if is_incremental() %}
  AND f.prblm_date >= CURRENT_DATE - INTERVAL '{{ var("watermark_lookback_days", 3) }}' DAY
{% endif %}
