{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  dim_date
  ──────────────────────────────────────────────────────────────────────────────
  Date dimension spanning 2020-01-01 through 2026-12-31 (2,557 days).
  Generated entirely in SQL using Trino's sequence() + UNNEST pattern —
  no external seed file required.

  day_of_week_num follows Trino's day_of_week() convention:
    1=Monday … 6=Saturday, 7=Sunday
*/

WITH date_spine AS (
  SELECT date_add('day', seq, DATE '2020-01-01') AS date_sk
  FROM (SELECT sequence(0, 2556) AS s)
  CROSS JOIN UNNEST(s) AS t(seq)
)

SELECT
  date_sk,
  year(date_sk)                                                   AS year,
  month(date_sk)                                                  AS month,
  day(date_sk)                                                    AS day,
  day_of_week(date_sk)                                            AS day_of_week_num,
  CASE day_of_week(date_sk)
    WHEN 1 THEN '週一'
    WHEN 2 THEN '週二'
    WHEN 3 THEN '週三'
    WHEN 4 THEN '週四'
    WHEN 5 THEN '週五'
    WHEN 6 THEN '週六'
    WHEN 7 THEN '週日'
  END                                                             AS day_of_week_name,
  CASE WHEN day_of_week(date_sk) IN (6, 7) THEN 1 ELSE 0 END     AS is_weekend,
  LPAD(CAST(year(date_sk)  AS VARCHAR), 4, '0')
    || LPAD(CAST(month(date_sk) AS VARCHAR), 2, '0')             AS year_month,
  format_datetime(date_sk, 'yyyy-Q')                              AS year_quarter

FROM date_spine
ORDER BY date_sk
