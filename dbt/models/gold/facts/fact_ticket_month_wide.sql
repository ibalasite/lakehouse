{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'merge',
    unique_key          = ['month_sk', 'catsub_id', 'prblm_source_id',
                           'prblm_class_id', 'prblm_perform_id', 'prblm_status_id'],
    on_schema_change    = 'sync_all_columns',
    schema              = 'gold'
  )
}}

/*
  fact_ticket_month_wide
  ──────────────────────────────────────────────────────────────────────────────
  Gold serving fact: monthly aggregate from fact_ticket_day_wide.

  2-month rolling lookback supports mid-month backfills and late arrivals.
  Weighted averages re-computed from sum/cnt components — never averaged
  over already-averaged values.

  Lineage:  fact_ticket_day_wide → fact_ticket_month_wide
*/

WITH src AS (
  SELECT *
  FROM {{ ref('fact_ticket_day_wide') }}
  {% if is_incremental() %}
  WHERE prblm_date >= CURRENT_DATE - INTERVAL '{{ var("month_wide_lookback_months", 2) }}' MONTH
  {% endif %}
)

SELECT
  -- ── Dimension keys ──────────────────────────────────────────────────────────
  CAST(YEAR(prblm_date) * 100 + MONTH(prblm_date) AS BIGINT) AS month_sk,
  catsub_id,
  prblm_source_id,
  prblm_class_id,
  prblm_perform_id,
  prblm_status_id,

  -- ── Volume metrics ───────────────────────────────────────────────────────────
  SUM(total_tickets)      AS total_tickets,
  SUM(resolved_tickets)   AS resolved_tickets,
  SUM(one_shot_resolved)  AS one_shot_resolved,
  SUM(complain_tickets)   AS complain_tickets,
  SUM(forwarded_tickets)  AS forwarded_tickets,
  SUM(within_sla_tickets) AS within_sla_tickets,

  -- ── Duration components (additive, for future re-aggregation) ───────────────
  SUM(sum_resolution_hours) AS sum_resolution_hours,
  SUM(cnt_resolution_hours) AS cnt_resolution_hours,
  SUM(sum_response_hours)   AS sum_response_hours,
  SUM(cnt_response_hours)   AS cnt_response_hours,

  -- ── Pre-computed metrics for BI convenience ──────────────────────────────────
  TRY_CAST(
    SUM(sum_resolution_hours) / NULLIF(SUM(cnt_resolution_hours), 0)
  AS DOUBLE) AS avg_resolution_hours,

  TRY_CAST(
    SUM(sum_response_hours) / NULLIF(SUM(cnt_response_hours), 0)
  AS DOUBLE) AS avg_response_hours,

  CAST(SUM(resolved_tickets)   AS DOUBLE) / NULLIF(CAST(SUM(total_tickets) AS DOUBLE), 0) * 100 AS pct_resolved,
  CAST(SUM(within_sla_tickets) AS DOUBLE) / NULLIF(CAST(SUM(total_tickets) AS DOUBLE), 0) * 100 AS pct_within_sla,
  CAST(SUM(one_shot_resolved)  AS DOUBLE) / NULLIF(CAST(SUM(total_tickets) AS DOUBLE), 0) * 100 AS pct_one_shot,

  -- ── Pipeline metadata ────────────────────────────────────────────────────────
  MAX(updated_at) AS updated_at

FROM src
GROUP BY
  CAST(YEAR(prblm_date) * 100 + MONTH(prblm_date) AS BIGINT),
  catsub_id,
  prblm_source_id,
  prblm_class_id,
  prblm_perform_id,
  prblm_status_id
