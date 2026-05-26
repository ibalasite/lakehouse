{{
  config(
    materialized         = 'incremental',
    table_type           = platform_iceberg_table_type(),
    incremental_strategy = platform_merge_strategy(),
    unique_key           = ['prblm_date', 'prblm_hour', 'catsub_id', 'prblm_source_id',
                            'prblm_class_id', 'prblm_perform_id', 'prblm_status_id'],
    on_schema_change     = 'ignore',
    schema               = 'cache'
  )
}}

/*
  cache_ticket_hourly
  ──────────────────────────────────────────────────────────────────────────────
  Dual-target Cache MV (hourly batch layer): fact_ticket_hour_wide LEFT JOIN all dims.
  Pre-joined flat table — BI queries need zero JOINs.

  Runs twice per streaming and daily DAG (EDD §13.2b):
    dbt run --select cache_ticket_hourly --target prod        → iceberg.cache
    dbt run --select cache_ticket_hourly --target mysql_cache → mysql.lakehouse_cache

  Incremental MERGE on grain keys: only processes last
  watermark_lookback_days (default 3) on incremental runs; full 730-day
  window on first run or --full-refresh.

  Lineage:  fact_ticket_hour_wide + dim_* → cache_ticket_hourly (both targets)
*/

SELECT
  -- ── Date + Hour ──────────────────────────────────────────────────────────────
  h.prblm_date,
  h.prblm_hour                          AS hour_of_day,

  -- ── Dimension keys (retained for filtering) ──────────────────────────────────
  h.catsub_id,
  h.prblm_source_id,
  h.prblm_class_id,
  h.prblm_perform_id,
  h.prblm_status_id,

  -- ── Dimension labels (pre-joined for BI) ─────────────────────────────────────
  cat.product_name,
  cat.region                            AS catsub_region,
  cat.catsub_code,
  src.source_name_zh,
  src.source_name_en,
  sts.status_name_zh,
  sts.status_name_en,
  perf.perform_name,
  perf.sla_hours,

  -- ── Volume metrics ───────────────────────────────────────────────────────────
  h.total_tickets,
  h.resolved_tickets,
  h.one_shot_resolved,
  h.complain_tickets,
  h.forwarded_tickets,
  h.within_sla_tickets,

  -- ── Duration components (additive — kept for correct cross-period weighted avg)
  h.sum_resolution_hours,
  h.cnt_resolution_hours,
  h.sum_response_hours,
  h.cnt_response_hours,

  -- ── Pre-computed metrics for BI convenience ──────────────────────────────────
  TRY_CAST(
    h.sum_resolution_hours / NULLIF(h.cnt_resolution_hours, 0)
  AS DOUBLE)                            AS avg_resolution_hours,
  TRY_CAST(
    h.sum_response_hours / NULLIF(h.cnt_response_hours, 0)
  AS DOUBLE)                            AS avg_response_hours,

  -- ── Freshness ────────────────────────────────────────────────────────────────
  h.updated_at

FROM {{ ref('fact_ticket_hour_wide') }}        AS h
LEFT JOIN {{ ref('dim_catsub') }}             AS cat  ON h.catsub_id       = cat.catsub_id
LEFT JOIN {{ ref('dim_prblm_source') }}       AS src  ON h.prblm_source_id  = src.prblm_source_id
LEFT JOIN {{ ref('dim_prblm_status') }}       AS sts  ON h.prblm_status_id  = sts.prblm_status_id
LEFT JOIN {{ ref('dim_perform') }}            AS perf ON h.prblm_perform_id  = perf.prblm_perform_id

WHERE h.prblm_date >= CURRENT_DATE - INTERVAL '730' DAY
{% if is_incremental() %}
  AND h.prblm_date >= CURRENT_DATE - INTERVAL '{{ var("watermark_lookback_days", 3) }}' DAY
{% endif %}
