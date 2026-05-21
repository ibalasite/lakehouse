{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'append',
    on_schema_change    = 'append_new_columns',
    schema              = 'gold'
  )
}}

/*
  fact_ticket_hour_long
  ──────────────────────────────────────────────────────────────────────────────
  Gold canonical fact: EAV narrow format, append-only.

  One row per (prblm_date, prblm_hour, dimension_combo, field_code).
  Each 15-min run appends only NEW silver rows (prblm_sysdate watermark).
  No full-table scan, no MERGE — eliminates OOMKill.

  Lineage:  stg_silver_tickets → fact_ticket_hour_long (canonical)
                               → fact_ticket_hour_wide (PIVOT, serving)
*/

WITH new_silver AS (
  SELECT
    prblm_date,
    CAST(EXTRACT(HOUR FROM (prblm_sysdate AT TIME ZONE 'Asia/Taipei')) AS INTEGER) AS prblm_hour,
    catsub_id,
    prblm_source_id,
    prblm_class_id,
    COALESCE(prblm_perform_id, 99)  AS prblm_perform_id,
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
  WHERE prblm_sysdate > (
    SELECT COALESCE(
      MAX(updated_at) - INTERVAL '1' MINUTE,
      TIMESTAMP '1900-01-01 00:00:00.000000 UTC'
    )
    FROM {{ this }}
  )
  {% endif %}
),

agg AS (
  SELECT
    prblm_date,
    prblm_hour,
    catsub_id,
    prblm_source_id,
    prblm_class_id,
    prblm_perform_id,
    prblm_status_id,
    COUNT(*)                                                                        AS total_tickets,
    SUM(is_resolved)                                                                AS resolved_tickets,
    SUM(CASE WHEN prblm_doneatatime THEN 1 ELSE 0 END)                              AS one_shot_resolved,
    SUM(is_complain)                                                                AS complain_tickets,
    SUM(is_forwarded)                                                               AS forwarded_tickets,
    SUM(within_sla)                                                                 AS within_sla_tickets,
    SUM(CASE WHEN resolution_hours >= 0 THEN CAST(resolution_hours AS DOUBLE) END) AS resolution_hours_sum,
    COUNT(CASE WHEN resolution_hours >= 0 THEN 1 END)                               AS resolution_hours_cnt,
    SUM(CASE WHEN response_hours >= 0 THEN CAST(response_hours AS DOUBLE) END)     AS response_hours_sum,
    COUNT(CASE WHEN response_hours >= 0 THEN 1 END)                                 AS response_hours_cnt
  FROM new_silver
  GROUP BY
    prblm_date, prblm_hour,
    catsub_id, prblm_source_id, prblm_class_id, prblm_perform_id, prblm_status_id
)

-- EAV unpivot: one row per (grain, field_code) — 10 metric fields
SELECT prblm_date, prblm_hour, catsub_id, prblm_source_id, prblm_class_id, prblm_perform_id, prblm_status_id,
    'total_tickets'                                             AS field_code,
    {{ stable_hash64_number("'ticket|total_tickets'") }}        AS field_sk,
    CAST(total_tickets AS DECIMAL(38,12))                       AS value_decimal,
    CAST(NULL AS DOUBLE)                                        AS value_double,
    current_timestamp                                           AS updated_at
FROM agg

UNION ALL

SELECT prblm_date, prblm_hour, catsub_id, prblm_source_id, prblm_class_id, prblm_perform_id, prblm_status_id,
    'resolved_tickets',
    {{ stable_hash64_number("'ticket|resolved_tickets'") }},
    CAST(resolved_tickets AS DECIMAL(38,12)), CAST(NULL AS DOUBLE), current_timestamp
FROM agg

UNION ALL

SELECT prblm_date, prblm_hour, catsub_id, prblm_source_id, prblm_class_id, prblm_perform_id, prblm_status_id,
    'one_shot_resolved',
    {{ stable_hash64_number("'ticket|one_shot_resolved'") }},
    CAST(one_shot_resolved AS DECIMAL(38,12)), CAST(NULL AS DOUBLE), current_timestamp
FROM agg

UNION ALL

SELECT prblm_date, prblm_hour, catsub_id, prblm_source_id, prblm_class_id, prblm_perform_id, prblm_status_id,
    'complain_tickets',
    {{ stable_hash64_number("'ticket|complain_tickets'") }},
    CAST(complain_tickets AS DECIMAL(38,12)), CAST(NULL AS DOUBLE), current_timestamp
FROM agg

UNION ALL

SELECT prblm_date, prblm_hour, catsub_id, prblm_source_id, prblm_class_id, prblm_perform_id, prblm_status_id,
    'forwarded_tickets',
    {{ stable_hash64_number("'ticket|forwarded_tickets'") }},
    CAST(forwarded_tickets AS DECIMAL(38,12)), CAST(NULL AS DOUBLE), current_timestamp
FROM agg

UNION ALL

SELECT prblm_date, prblm_hour, catsub_id, prblm_source_id, prblm_class_id, prblm_perform_id, prblm_status_id,
    'within_sla_tickets',
    {{ stable_hash64_number("'ticket|within_sla_tickets'") }},
    CAST(within_sla_tickets AS DECIMAL(38,12)), CAST(NULL AS DOUBLE), current_timestamp
FROM agg

UNION ALL

SELECT prblm_date, prblm_hour, catsub_id, prblm_source_id, prblm_class_id, prblm_perform_id, prblm_status_id,
    'resolution_hours_sum',
    {{ stable_hash64_number("'ticket|resolution_hours_sum'") }},
    CAST(NULL AS DECIMAL(38,12)), resolution_hours_sum, current_timestamp
FROM agg

UNION ALL

SELECT prblm_date, prblm_hour, catsub_id, prblm_source_id, prblm_class_id, prblm_perform_id, prblm_status_id,
    'resolution_hours_cnt',
    {{ stable_hash64_number("'ticket|resolution_hours_cnt'") }},
    CAST(resolution_hours_cnt AS DECIMAL(38,12)), CAST(NULL AS DOUBLE), current_timestamp
FROM agg

UNION ALL

SELECT prblm_date, prblm_hour, catsub_id, prblm_source_id, prblm_class_id, prblm_perform_id, prblm_status_id,
    'response_hours_sum',
    {{ stable_hash64_number("'ticket|response_hours_sum'") }},
    CAST(NULL AS DECIMAL(38,12)), response_hours_sum, current_timestamp
FROM agg

UNION ALL

SELECT prblm_date, prblm_hour, catsub_id, prblm_source_id, prblm_class_id, prblm_perform_id, prblm_status_id,
    'response_hours_cnt',
    {{ stable_hash64_number("'ticket|response_hours_cnt'") }},
    CAST(response_hours_cnt AS DECIMAL(38,12)), CAST(NULL AS DOUBLE), current_timestamp
FROM agg
