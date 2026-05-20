{{
  config(
    materialized = 'table',
    database     = 'mysql',
    schema       = 'lakehouse_cache'
  )
}}

/*
  cache_ticket_daily
  ──────────────────────────────────────────────────────────────────────────────
  Cache layer: daily ticket metrics materialized as a TABLE and written to the
  MySQL lakehouse_cache database via Trino's MySQL catalog connector.

  Scope: trailing 730 days (2 calendar years) — sufficient for all operational
  dashboards while keeping the MySQL table small enough for sub-second queries.

  Run command:
    dbt run --select cache_ticket_daily --target mysql_cache

  Why TABLE not incremental:
    MySQL via Trino's connector does not support MERGE DML, so a full replace
    on each run is safer and simpler. The 730-day window reads ~500K rows from
    the gold fact table — a full refresh completes in seconds.

  Freshness SLA: this table should be refreshed within 15 minutes of
  fact_ticket_day_wide completing its incremental run.
*/

SELECT
  prblm_date          AS date_sk,
  catsub_id,
  prblm_source_id,
  prblm_class_id,
  prblm_perform_id,
  prblm_status_id,
  total_tickets,
  resolved_tickets,
  one_shot_resolved,
  complain_tickets,
  forwarded_tickets,
  within_sla_tickets,
  avg_resolution_hours,
  avg_response_hours,
  pct_resolved,
  pct_within_sla,
  pct_one_shot,
  updated_at

-- Cross-catalog reference: always read from Iceberg gold regardless of target.
-- We bypass {{ ref() }} to avoid dbt resolving this to the mysql catalog.
FROM iceberg.gold.fact_ticket_day_wide
WHERE prblm_date >= CURRENT_DATE - INTERVAL '730' DAY
