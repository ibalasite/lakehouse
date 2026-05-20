{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'merge',
    unique_key          = ['prblm_date', 'catsub_id', 'prblm_source_id',
                           'prblm_class_id', 'prblm_perform_id', 'prblm_status_id'],
    on_schema_change    = 'sync_all_columns',
    schema              = 'gold'
  )
}}

/*
  fact_ticket_day_wide
  ──────────────────────────────────────────────────────────────────────────────
  Gold fact table: daily ticket aggregations by key dimensions.

  Granularity: one row per (prblm_date, catsub_id, prblm_source_id,
               prblm_class_id, prblm_perform_id, prblm_status_id).

  Incremental strategy: MERGE on the composite key.
  Watermark: reprocess the trailing {{ var('watermark_lookback_days', 3) }}
  calendar days on every incremental run to absorb late-arriving silver
  updates (status changes, SLA recalculations) without a full rebuild.

  NULL perform_id is coalesced to 99 (一般/standard tier) so every row joins
  cleanly to dim_perform without LEFT JOIN fanout.

  All percentage metrics use NULLIF on the denominator to prevent
  division-by-zero; result is NULL (not 0) when the group has no tickets,
  which is correct for BI tools that distinguish "no data" from "0%".
*/

WITH silver AS (
  SELECT
    prblm_date,
    catsub_id,
    prblm_source_id,
    prblm_class_id,
    COALESCE(prblm_perform_id, 99)  AS prblm_perform_id,   -- NULL → 99 (一般)
    prblm_status_id,
    is_resolved,
    prblm_doneatatime,
    is_complain,
    is_forwarded,
    within_sla,
    resolution_hours,
    response_hours
  FROM {{ ref('stg_silver_tickets') }}
  {% if is_incremental() %}
  WHERE prblm_date >= CURRENT_DATE - INTERVAL '{{ var("watermark_lookback_days", 3) }}' DAY
  {% endif %}
)

SELECT
  -- ── Dimension keys ──────────────────────────────────────────────────────────
  prblm_date,
  catsub_id,
  prblm_source_id,
  prblm_class_id,
  prblm_perform_id,
  prblm_status_id,

  -- ── Volume metrics ───────────────────────────────────────────────────────────
  COUNT(*)                                                          AS total_tickets,
  SUM(is_resolved)                                                  AS resolved_tickets,
  SUM(CASE WHEN prblm_doneatatime THEN 1 ELSE 0 END)               AS one_shot_resolved,
  SUM(is_complain)                                                  AS complain_tickets,
  SUM(is_forwarded)                                                 AS forwarded_tickets,
  SUM(within_sla)                                                   AS within_sla_tickets,

  -- ── Duration metrics (exclude negative values: data quality guard) ───────────
  AVG(CASE WHEN resolution_hours >= 0 THEN CAST(resolution_hours AS DOUBLE) END)
                                                                    AS avg_resolution_hours,
  AVG(CASE WHEN response_hours   >= 0 THEN CAST(response_hours   AS DOUBLE) END)
                                                                    AS avg_response_hours,

  -- ── Percentage metrics ───────────────────────────────────────────────────────
  CAST(SUM(is_resolved) AS DOUBLE)
    / NULLIF(COUNT(*), 0) * 100                                     AS pct_resolved,

  CAST(SUM(within_sla) AS DOUBLE)
    / NULLIF(COUNT(*), 0) * 100                                     AS pct_within_sla,

  CAST(SUM(CASE WHEN prblm_doneatatime THEN 1 ELSE 0 END) AS DOUBLE)
    / NULLIF(COUNT(*), 0) * 100                                     AS pct_one_shot,

  -- ── Pipeline metadata ────────────────────────────────────────────────────────
  current_timestamp                                                 AS updated_at

FROM silver
GROUP BY
  prblm_date,
  catsub_id,
  prblm_source_id,
  prblm_class_id,
  prblm_perform_id,
  prblm_status_id
