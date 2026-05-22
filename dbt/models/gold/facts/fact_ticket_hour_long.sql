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

  CROSS JOIN UNNEST replaces the original 10 UNION ALL pattern to avoid
  Trino re-executing the GROUP BY CTE 10 times (OOMKill on 10M-row silver).

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
  WHERE
    -- Static window: Trino pushes this to Iceberg file-level min/max stats on updated_at,
    -- pruning bulk silver files so only recent files are read. The subquery watermark
    -- below is NOT pushed to Iceberg (correlated scalar), so this guard is essential
    -- for preventing full silver table scan on 10M-row tables.
    updated_at >= date_add('hour', -{{ var('bronze_lookback_hours', 6) | int }}, current_timestamp)
    AND prblm_sysdate > (
      SELECT COALESCE(
        MAX(updated_at) - INTERVAL '1' MINUTE,
        TIMESTAMP '1900-01-01 00:00:00.000000 UTC'
      )
      FROM {{ this }}
    )
  {% else %}
  -- Full-refresh initial load: limit to last 30 days.
  -- 90-day initial load OOMs on 10GB dev node (4M rows too large for CTAS).
  -- Subsequent incremental appends cover all future arrivals.
  WHERE prblm_date >= CURRENT_DATE - INTERVAL '30' DAY
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

-- CROSS JOIN UNNEST: single scan of agg → 10 EAV rows per group.
-- Eliminates the original 10 UNION ALL pattern that caused Trino to
-- re-execute the GROUP BY 10 times and OOMKill on large silver tables.
SELECT
    prblm_date,
    prblm_hour,
    catsub_id,
    prblm_source_id,
    prblm_class_id,
    prblm_perform_id,
    prblm_status_id,
    field_code,
    {{ stable_hash64_number("'ticket|' || field_code") }} AS field_sk,
    value_decimal,
    value_double,
    current_timestamp AS updated_at
FROM agg
CROSS JOIN UNNEST(
    -- metric name array
    ARRAY[
        'total_tickets',
        'resolved_tickets',
        'one_shot_resolved',
        'complain_tickets',
        'forwarded_tickets',
        'within_sla_tickets',
        'resolution_hours_sum',
        'resolution_hours_cnt',
        'response_hours_sum',
        'response_hours_cnt'
    ],
    -- value_decimal array  (NULL for hour/response _sum which go into value_double)
    ARRAY[
        CAST(total_tickets          AS DECIMAL(38,12)),
        CAST(resolved_tickets       AS DECIMAL(38,12)),
        CAST(one_shot_resolved      AS DECIMAL(38,12)),
        CAST(complain_tickets       AS DECIMAL(38,12)),
        CAST(forwarded_tickets      AS DECIMAL(38,12)),
        CAST(within_sla_tickets     AS DECIMAL(38,12)),
        CAST(NULL                   AS DECIMAL(38,12)),
        CAST(resolution_hours_cnt   AS DECIMAL(38,12)),
        CAST(NULL                   AS DECIMAL(38,12)),
        CAST(response_hours_cnt     AS DECIMAL(38,12))
    ],
    -- value_double array  (non-NULL only for *_sum metrics)
    ARRAY[
        CAST(NULL AS DOUBLE),
        CAST(NULL AS DOUBLE),
        CAST(NULL AS DOUBLE),
        CAST(NULL AS DOUBLE),
        CAST(NULL AS DOUBLE),
        CAST(NULL AS DOUBLE),
        resolution_hours_sum,
        CAST(NULL AS DOUBLE),
        response_hours_sum,
        CAST(NULL AS DOUBLE)
    ]
) AS t(field_code, value_decimal, value_double)
