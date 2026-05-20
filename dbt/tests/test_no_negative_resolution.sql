-- Test: no negative resolution_hours for tickets marked as resolved.
--
-- A ticket with is_resolved=1 must have resolution_hours >= 0.
-- Negative resolution_hours on a resolved ticket indicates a data quality
-- issue in the source system (e.g. prblm_donedate earlier than prblm_sysdate)
-- or a bronze computation error.
--
-- This test returns 0 rows on success.
-- Any row returned is a failure that must be investigated before gold refresh.

SELECT
  prblm_code,
  prblm_sysdate,
  prblm_donedate,
  resolution_hours,
  is_resolved
FROM {{ ref('stg_silver_tickets') }}
WHERE is_resolved = 1
  AND resolution_hours < 0
