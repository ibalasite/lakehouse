{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'append',
    on_schema_change    = 'append_new_columns',
    schema              = 'bronze',
    properties          = {"format": "'PARQUET'"}
  )
}}

/*
  stg_bronze_tickets
  ──────────────────────────────────────────────────────────────────────────────
  Bronze layer: append-only raw ingest from iceberg.bronze.raw_tickets.

  Incremental strategy: APPEND.
  Watermark: ingested_at — the pipeline ingest timestamp set by the
  Kafka→Iceberg job. On incremental runs we load rows whose ingested_at
  exceeds the maximum already present in this table.

  PII handling applied at this layer:
  • prblm_name  → mask_name()        (first char + ***)
  • usr_id      → pii_hash()         (non-negative INT64 surrogate key)
  • usr_id_masked                    (first 2 + *** + last 2 chars)

  No transformation of business logic occurs here; all CASE logic (SLA,
  resolution) is kept minimal to avoid coupling the raw record to policy
  that may change independently.
*/

WITH source AS (
  SELECT *
  FROM {{ source('bronze', 'raw_tickets') }}
  WHERE
    -- Static window: Trino pushes this to Iceberg file-level min/max stats,
    -- pruning bulk files so only recent files are read. The subquery watermark
    -- below is NOT pushed to Iceberg (it's a correlated scalar), so this
    -- static guard is essential for preventing full 711MB raw_tickets scan.
    ingested_at >= date_add('hour', -{{ var('bronze_lookback_hours', 6) | int }}, current_timestamp)
    {% if is_incremental() %}
    -- Dynamic watermark: skip rows already in bronze. Combined with the static
    -- window above, only files within the window AND newer than the watermark
    -- are actually read row-by-row.
    AND ingested_at > (
      SELECT COALESCE(
        MAX(ingested_at) - INTERVAL '10' MINUTE,
        TIMESTAMP '2020-01-01 00:00:00'
      )
      FROM {{ this }}
    )
    {% endif %}
),

-- Compute raw hour deltas once so they can be reused in both the metric
-- columns and the derived flag columns without redundant date_diff calls.
with_durations AS (
  SELECT
    *,
    CASE
      WHEN prblm_donedate IS NOT NULL
        THEN date_diff('hour', prblm_sysdate, prblm_donedate)
      ELSE NULL
    END AS _resolution_hours_raw,

    CASE
      WHEN prblm_preassignenddate IS NOT NULL
        THEN date_diff('hour', prblm_sysdate, prblm_preassignenddate)
      ELSE NULL
    END AS _response_hours_raw

  FROM source
),

transformed AS (
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

    -- ── PII transformations ──────────────────────────────────────────────────
    {{ mask_name('prblm_name') }}                             AS customer_name_masked,
    {{ pii_hash('usr_id') }}                                  AS usr_pk,

    -- Partial masking: keep first 2 and last 2 characters of usr_id.
    -- Trino substr(string, position, length) is 1-indexed.
    -- For a string of length N: last-2 chars start at position (N - 1).
    -- Strings of 4 chars or fewer are fully masked to avoid exposing the
    -- full value through the unmasked prefix+suffix.
    CASE
      WHEN usr_id IS NULL           THEN NULL
      WHEN length(usr_id) <= 4     THEN '***'
      ELSE concat(
             substr(usr_id, 1, 2),
             '***',
             substr(usr_id, length(usr_id) - 1, 2)  -- Trino: 1-indexed, last 2 chars
           )
    END                                                       AS usr_id_masked,

    -- ── Other dimensions ─────────────────────────────────────────────────────
    gd_id,
    catsub_id,
    supplier_id,

    -- ── Flags ────────────────────────────────────────────────────────────────
    prblm_doneatatime,
    prblm_forwarddatetime,
    prblm_forwardtype,
    prblm_notasreason_id,
    prblm_notasowndept_id,

    -- ── Computed metrics (reference pre-computed durations, no repeated date_diff) ──
    _resolution_hours_raw                                     AS resolution_hours,
    _response_hours_raw                                       AS response_hours,

    -- is_resolved: 1 when closed with a non-negative resolution duration
    CASE
      WHEN _resolution_hours_raw IS NOT NULL
       AND _resolution_hours_raw >= 0 THEN 1
      ELSE 0
    END                                                       AS is_resolved,

    -- within_sla: 1 when first response fell within the tier's SLA window.
    --   特急 (perform_id=2): must respond within 4 hours
    --   急件 (perform_id=1): must respond within 8 hours
    --   一般 (NULL):         must respond within 24 hours
    CASE
      WHEN _response_hours_raw IS NULL THEN 0
      WHEN prblm_perform_id = 2  AND _response_hours_raw <= 4  THEN 1
      WHEN prblm_perform_id = 1  AND _response_hours_raw <= 8  THEN 1
      WHEN prblm_perform_id IS NULL AND _response_hours_raw <= 24 THEN 1
      ELSE 0
    END                                                       AS within_sla,

    -- ── Pipeline metadata (EDD §8.2) ─────────────────────────────────────────
    ingested_at,
    current_timestamp                                         AS bronze_created_at,
    -- EDD §8.2 mandatory audit columns
    CAST(ingested_at AT TIME ZONE 'Asia/Taipei' AS DATE)      AS _ingested_date,
    CAST('data_source_pod' AS VARCHAR)                        AS _source_system,
    CAST('raw_tickets' AS VARCHAR)                            AS _source_table,
    CAST('/api/tickets' AS VARCHAR)                           AS _source_file_path,
    CAST('r' AS VARCHAR)                                      AS _cdc_op,
    ingested_at                                               AS _cdc_ts,
    CAST('{{ invocation_id }}' AS VARCHAR)                    AS _batch_id,
    {{ stable_hash64_number("prblm_code || '|' || cast(coalesce(prblm_updateddate, TIMESTAMP '1900-01-01 00:00:00 UTC') as varchar)") }} AS _record_hash

  FROM with_durations
)

SELECT * FROM transformed
