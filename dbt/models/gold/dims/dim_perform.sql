{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  dim_perform
  ──────────────────────────────────────────────────────────────────────────────
  Static lookup table for SLA tier (perform) codes.

  prblm_perform_id=99 is a synthetic surrogate for the standard (一般) tier,
  which is represented as NULL in the source system. Fact tables coalesce
  NULL perform_id to 99 before joining to this dimension so that standard-tier
  tickets are not lost in LEFT JOIN fanout.
*/

SELECT *
FROM (
  VALUES
    (1,  '急件', 8,  '8hr內回覆'),
    (2,  '特急', 4,  '4hr內回覆'),
    (99, '一般', 24, '24hr內回覆')
) AS t(prblm_perform_id, perform_name, sla_hours, sla_desc)
