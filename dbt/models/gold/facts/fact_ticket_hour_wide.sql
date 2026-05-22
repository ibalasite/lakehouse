{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'merge',
    unique_key          = ['prblm_date', 'prblm_hour', 'catsub_id', 'prblm_source_id',
                           'prblm_class_id', 'prblm_perform_id', 'prblm_status_id'],
    on_schema_change    = 'sync_all_columns',
    schema              = 'gold'
  )
}}

/*
  fact_ticket_hour_wide
  ──────────────────────────────────────────────────────────────────────────────
  Gold serving fact: hourly PIVOT from fact_ticket_hour_long (EAV narrow).

  One row per (prblm_date, prblm_hour, dimension_combo).
  Watermark reads only NEW delta rows from hour_long since last update.
  MERGE upserts cumulative totals — no full silver scan.

  Lineage:  fact_ticket_hour_long → fact_ticket_hour_wide (PIVOT)
                                  → fact_ticket_day_wide (aggregate)
*/

WITH src AS (
  SELECT *
  FROM {{ ref('fact_ticket_hour_long') }}
  {% if is_incremental() %}
  WHERE
    -- Static window: prunes hour_long Iceberg files before the subquery watermark.
    updated_at >= date_add('hour', -{{ var('bronze_lookback_hours', 6) | int }}, current_timestamp)
    AND updated_at > (
      SELECT COALESCE(
        MAX(updated_at),
        TIMESTAMP '1900-01-01 00:00:00.000000 UTC'
      )
      FROM {{ this }}
    )
  {% endif %}
)

SELECT
  -- ── Dimension keys ──────────────────────────────────────────────────────────
  prblm_date,
  prblm_hour,
  catsub_id,
  prblm_source_id,
  prblm_class_id,
  prblm_perform_id,
  prblm_status_id,

  -- ── Volume metrics (PIVOT by field_code) ────────────────────────────────────
  SUM(CASE WHEN field_code = 'total_tickets'    THEN value_decimal ELSE CAST(0 AS DECIMAL(38,12)) END) AS total_tickets,
  SUM(CASE WHEN field_code = 'resolved_tickets' THEN value_decimal ELSE CAST(0 AS DECIMAL(38,12)) END) AS resolved_tickets,
  SUM(CASE WHEN field_code = 'one_shot_resolved' THEN value_decimal ELSE CAST(0 AS DECIMAL(38,12)) END) AS one_shot_resolved,
  SUM(CASE WHEN field_code = 'complain_tickets'  THEN value_decimal ELSE CAST(0 AS DECIMAL(38,12)) END) AS complain_tickets,
  SUM(CASE WHEN field_code = 'forwarded_tickets' THEN value_decimal ELSE CAST(0 AS DECIMAL(38,12)) END) AS forwarded_tickets,
  SUM(CASE WHEN field_code = 'within_sla_tickets' THEN value_decimal ELSE CAST(0 AS DECIMAL(38,12)) END) AS within_sla_tickets,

  -- ── Duration components (sum + cnt for correct weighted avg at day level) ───
  SUM(CASE WHEN field_code = 'resolution_hours_sum' THEN value_double  ELSE CAST(0 AS DOUBLE) END) AS sum_resolution_hours,
  SUM(CASE WHEN field_code = 'resolution_hours_cnt' THEN value_decimal ELSE CAST(0 AS DECIMAL(38,12)) END) AS cnt_resolution_hours,
  SUM(CASE WHEN field_code = 'response_hours_sum'   THEN value_double  ELSE CAST(0 AS DOUBLE) END) AS sum_response_hours,
  SUM(CASE WHEN field_code = 'response_hours_cnt'   THEN value_decimal ELSE CAST(0 AS DECIMAL(38,12)) END) AS cnt_response_hours,

  -- ── Pipeline metadata ────────────────────────────────────────────────────────
  MAX(updated_at) AS updated_at

FROM src
GROUP BY
  prblm_date,
  prblm_hour,
  catsub_id,
  prblm_source_id,
  prblm_class_id,
  prblm_perform_id,
  prblm_status_id
