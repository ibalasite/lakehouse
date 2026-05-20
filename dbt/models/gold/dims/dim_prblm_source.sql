{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  dim_prblm_source
  ──────────────────────────────────────────────────────────────────────────────
  Static lookup table for contact channel (source) codes.
  Source of truth for prblm_source_id values used across all fact tables.
*/

SELECT *
FROM (
  VALUES
    (1, 'Email',  '電子郵件'),
    (2, 'Chat',   '即時對話'),
    (3, 'Phone',  '電話'),
    (4, 'Portal', '客服入口'),
    (5, 'App',    '行動應用')
) AS t(prblm_source_id, source_name_en, source_name_zh)
