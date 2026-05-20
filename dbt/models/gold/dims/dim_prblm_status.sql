{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  dim_prblm_status
  ──────────────────────────────────────────────────────────────────────────────
  Static lookup table for ticket lifecycle status codes.
  Source of truth for prblm_status_id values used across all fact tables.
*/

SELECT *
FROM (
  VALUES
    (1, '開啟',   'OPEN'),
    (2, '處理中', 'IN_PROGRESS'),
    (3, '待回覆', 'PENDING_REPLY'),
    (4, '已回覆', 'REPLIED'),
    (5, '結案',   'CLOSED')
) AS t(prblm_status_id, status_name_zh, status_name_en)
