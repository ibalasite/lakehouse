{{
  config(
    materialized = 'table',
    schema       = 'cache'
  )
}}

/*
  cache_daily_report
  ──────────────────────────────────────────────────────────────────────────────
  Lambda Architecture UNION ALL serving MV — primary table Metabase reads.

  Batch layer  (D-1 and before): fixed daily aggregates from fact_ticket_day_wide.
  Speed layer  (today D+0):      intraday SUM of fact_ticket_hour_wide.

  Both branches LEFT JOIN all dim tables for zero-JOIN BI queries.
  source_tier column distinguishes 'batch' vs 'intraday_hourly_sum'.

  Runs twice per cycle (§13.2b):
    dbt run --select cache_daily_report --target prod        → iceberg.cache
    dbt run --select cache_daily_report --target mysql_cache → mysql.lakehouse_cache

  Full table replace on each run. Streaming DAG refreshes every 15 min.

  Lineage:
    iceberg.gold.fact_ticket_day_wide  (D-1 and before) ─┐
    iceberg.gold.fact_ticket_hour_wide (today SUM)       ─┴→ cache_daily_report
*/

-- ── Batch layer: D-1 and before ─────────────────────────────────────────────
SELECT
  f.prblm_date                         AS date_sk,
  f.catsub_id,
  f.prblm_source_id,
  f.prblm_class_id,
  f.prblm_perform_id,
  f.prblm_status_id,
  cat.product_name,
  cat.region                           AS catsub_region,
  cat.catsub_code,
  src.source_name_zh,
  src.source_name_en,
  sts.status_name_zh,
  sts.status_name_en,
  perf.perform_name,
  perf.sla_hours,
  f.total_tickets,
  f.resolved_tickets,
  f.one_shot_resolved,
  f.complain_tickets,
  f.forwarded_tickets,
  f.within_sla_tickets,
  f.avg_resolution_hours,
  f.avg_response_hours,
  f.pct_resolved,
  f.pct_within_sla,
  f.pct_one_shot,
  'batch'                              AS source_tier,
  f.prblm_date                         AS freshness_as_of,
  f.updated_at

FROM {{ ref('fact_ticket_day_wide') }}        AS f
LEFT JOIN {{ ref('dim_catsub') }}             AS cat  ON f.catsub_id       = cat.catsub_id
LEFT JOIN {{ ref('dim_prblm_source') }}       AS src  ON f.prblm_source_id  = src.prblm_source_id
LEFT JOIN {{ ref('dim_prblm_status') }}       AS sts  ON f.prblm_status_id  = sts.prblm_status_id
LEFT JOIN {{ ref('dim_perform') }}            AS perf ON f.prblm_perform_id  = perf.prblm_perform_id

WHERE f.prblm_date >= CURRENT_DATE - INTERVAL '730' DAY
  AND f.prblm_date < CAST(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Taipei' AS DATE)

UNION ALL

-- ── Speed layer: today intraday SUM from hour_wide ───────────────────────────
SELECT
  h.prblm_date                                                             AS date_sk,
  h.catsub_id,
  h.prblm_source_id,
  h.prblm_class_id,
  h.prblm_perform_id,
  h.prblm_status_id,
  cat.product_name,
  cat.region                                                               AS catsub_region,
  cat.catsub_code,
  src.source_name_zh,
  src.source_name_en,
  sts.status_name_zh,
  sts.status_name_en,
  perf.perform_name,
  perf.sla_hours,
  CAST(SUM(h.total_tickets)      AS DECIMAL(38,12))                        AS total_tickets,
  CAST(SUM(h.resolved_tickets)   AS DECIMAL(38,12))                        AS resolved_tickets,
  CAST(SUM(h.one_shot_resolved)  AS DECIMAL(38,12))                        AS one_shot_resolved,
  CAST(SUM(h.complain_tickets)   AS DECIMAL(38,12))                        AS complain_tickets,
  CAST(SUM(h.forwarded_tickets)  AS DECIMAL(38,12))                        AS forwarded_tickets,
  CAST(SUM(h.within_sla_tickets) AS DECIMAL(38,12))                        AS within_sla_tickets,
  TRY_CAST(
    SUM(h.sum_resolution_hours) / NULLIF(SUM(h.cnt_resolution_hours), 0)
  AS DOUBLE)                                                               AS avg_resolution_hours,
  TRY_CAST(
    SUM(h.sum_response_hours) / NULLIF(SUM(h.cnt_response_hours), 0)
  AS DOUBLE)                                                               AS avg_response_hours,
  CAST(SUM(h.resolved_tickets)   AS DOUBLE)
    / NULLIF(CAST(SUM(h.total_tickets) AS DOUBLE), 0) * 100               AS pct_resolved,
  CAST(SUM(h.within_sla_tickets) AS DOUBLE)
    / NULLIF(CAST(SUM(h.total_tickets) AS DOUBLE), 0) * 100               AS pct_within_sla,
  CAST(SUM(h.one_shot_resolved)  AS DOUBLE)
    / NULLIF(CAST(SUM(h.total_tickets) AS DOUBLE), 0) * 100               AS pct_one_shot,
  'intraday_hourly_sum'                                                    AS source_tier,
  h.prblm_date                                                             AS freshness_as_of,
  MAX(h.updated_at)                                                        AS updated_at

FROM {{ ref('fact_ticket_hour_wide') }}       AS h
LEFT JOIN {{ ref('dim_catsub') }}             AS cat  ON h.catsub_id       = cat.catsub_id
LEFT JOIN {{ ref('dim_prblm_source') }}       AS src  ON h.prblm_source_id  = src.prblm_source_id
LEFT JOIN {{ ref('dim_prblm_status') }}       AS sts  ON h.prblm_status_id  = sts.prblm_status_id
LEFT JOIN {{ ref('dim_perform') }}            AS perf ON h.prblm_perform_id  = perf.prblm_perform_id

WHERE h.prblm_date = CAST(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Taipei' AS DATE)

GROUP BY
  h.prblm_date,
  h.catsub_id,
  h.prblm_source_id,
  h.prblm_class_id,
  h.prblm_perform_id,
  h.prblm_status_id,
  cat.product_name,
  cat.region,
  cat.catsub_code,
  src.source_name_zh,
  src.source_name_en,
  sts.status_name_zh,
  sts.status_name_en,
  perf.perform_name,
  perf.sla_hours
