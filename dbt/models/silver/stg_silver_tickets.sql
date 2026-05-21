{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'append',
    on_schema_change    = 'append_new_columns',
    schema              = 'silver'
  )
}}

/*
  stg_silver_tickets
  ──────────────────────────────────────────────────────────────────────────────
  Silver layer: cleansed, deduplicated, and conformed ticket records.

  Incremental strategy: MERGE on prblm_code.
  Watermark: bronze_created_at — we pull bronze rows written since the latest
  silver updated_at, ensuring CDC replays and late-arriving bronze rows are
  picked up correctly.

  Deduplication: within each incremental batch, tickets may appear multiple
  times from CDC replay. ROW_NUMBER() over prblm_code ordered by
  prblm_updateddate DESC keeps only the most recent source record.

  Enrichments added at this layer:
  • resolution_bucket  — Chinese time-band label for resolution SLA reporting
  • response_bucket    — Chinese time-band label for first-response reporting
  • is_complain        — escalation flag (complain_id >= 2)
  • is_forwarded       — forwarded-to-another-team flag
  • prblm_date         — DATE partition key for gold fact aggregations
*/

-- Macro dependencies (resolved at dbt compile time, not runtime):
--   resolution_bucket  → macros/platform/resolution_bucket.sql
--   response_bucket    → macros/platform/response_bucket.sql

WITH bronze_incremental AS (
  SELECT
    -- Enumerate all columns explicitly to make the schema contract visible
    -- and prevent unintended column additions from silently propagating.
    prblm_code,
    prblm_sysdate,
    prblm_updateddate,
    prblm_donedate,
    prblm_preassignenddate,
    prblm_preassignbegindate,
    prblm_status_id,
    prblm_source_id,
    prblm_class_id,
    prblm_intclass_id,
    prblm_perform_id,
    prblm_complain_id,
    prblm_processuser,
    prblm_doneuser,
    prblm_sysuser,
    customer_name_masked,
    usr_pk,
    usr_id_masked,
    gd_id,
    catsub_id,
    supplier_id,
    prblm_doneatatime,
    prblm_forwarddatetime,
    prblm_forwardtype,
    prblm_notasreason_id,
    prblm_notasowndept_id,
    resolution_hours,
    response_hours,
    is_resolved,
    within_sla,
    ingested_at,
    bronze_created_at
  FROM {{ ref('stg_bronze_tickets') }}
  {% if is_incremental() %}
  WHERE
    -- Guard against NULL bronze_created_at (data quality issue in upstream).
    -- Such rows are excluded here; they will be caught by the bronze
    -- not_null test on bronze_created_at and surfaced as a pipeline alert.
    bronze_created_at IS NOT NULL
    AND bronze_created_at >= (
      SELECT COALESCE(
        MAX(updated_at) - INTERVAL '10' MINUTE,
        TIMESTAMP '2020-01-01 00:00:00'
      )
      FROM {{ this }}
    )
  {% endif %}
),

-- Deduplicate: keep the most recently updated version of each ticket
-- within the current incremental batch. On full refresh this deduplicates
-- the entire bronze history.
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY prblm_code
      ORDER BY prblm_updateddate DESC NULLS LAST
    ) AS _row_rank
  FROM bronze_incremental
),

silver AS (
  SELECT
    -- ── Business key ────────────────────────────────────────────────────────
    prblm_code,

    -- ── Timestamps ──────────────────────────────────────────────────────────
    prblm_sysdate,
    prblm_updateddate,
    prblm_donedate,
    prblm_preassignenddate,
    prblm_preassignbegindate,

    -- ── Categorical dimensions ───────────────────────────────────────────────
    prblm_status_id,
    prblm_source_id,
    prblm_class_id,
    prblm_intclass_id,
    prblm_perform_id,
    prblm_complain_id,

    -- ── Agent / user references ──────────────────────────────────────────────
    prblm_processuser,
    prblm_doneuser,
    prblm_sysuser,

    -- ── PII (already masked/hashed in bronze) ────────────────────────────────
    customer_name_masked,
    usr_pk,
    usr_id_masked,

    -- ── Other dimensions ─────────────────────────────────────────────────────
    gd_id,
    catsub_id,
    supplier_id,

    -- ── Flags from bronze ────────────────────────────────────────────────────
    prblm_doneatatime,
    prblm_forwarddatetime,
    prblm_forwardtype,
    prblm_notasreason_id,
    prblm_notasowndept_id,

    -- ── Metrics from bronze ──────────────────────────────────────────────────
    resolution_hours,
    response_hours,
    is_resolved,
    within_sla,

    -- ── Silver enrichments ───────────────────────────────────────────────────
    {{ resolution_bucket('resolution_hours') }}               AS resolution_bucket,
    {{ response_bucket('response_hours') }}                   AS response_bucket,

    CASE WHEN prblm_complain_id >= 2 THEN 1 ELSE 0 END        AS is_complain,

    CASE WHEN prblm_forwarddatetime IS NOT NULL THEN 1 ELSE 0 END AS is_forwarded,

    -- DATE partition key used by gold fact aggregations (Asia/Taipei)
    CAST(prblm_sysdate AT TIME ZONE 'Asia/Taipei' AS DATE)    AS prblm_date,

    -- ── Pipeline metadata ────────────────────────────────────────────────────
    ingested_at,
    current_timestamp                                         AS updated_at

  FROM deduped
  WHERE _row_rank = 1
)

SELECT * FROM silver
