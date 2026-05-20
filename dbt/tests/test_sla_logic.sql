-- Test: within_sla cannot be 1 when response_hours is NULL.
--
-- If within_sla=1 it means we computed that the first response occurred
-- within the SLA window. That computation requires a non-NULL response_hours.
-- A row with within_sla=1 AND response_hours IS NULL indicates a logic error
-- in the bronze within_sla CASE expression.
--
-- This test returns 0 rows on success.
-- Any row returned indicates a bug in stg_bronze_tickets.within_sla logic.

SELECT
  prblm_code,
  prblm_sysdate,
  prblm_preassignenddate,
  prblm_perform_id,
  response_hours,
  within_sla
FROM {{ ref('stg_bronze_tickets') }}
WHERE within_sla = 1
  AND response_hours IS NULL
