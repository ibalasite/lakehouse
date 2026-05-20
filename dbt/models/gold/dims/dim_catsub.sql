{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  dim_catsub
  ──────────────────────────────────────────────────────────────────────────────
  Static lookup table for product line sub-site codes.
  Source of truth for catsub_id values used across all fact tables.
*/

SELECT *
FROM (
  VALUES
    (1,  '購物', '台灣', 'shopping_tw'),
    (2,  '購物', '香港', 'shopping_hk'),
    (3,  '拍賣', '台灣', 'auction_tw'),
    (4,  '超市', '台灣', 'superstore_tw'),
    (5,  '旅遊', '台灣', 'travel_tw'),
    (6,  '票券', '台灣', 'ticket_tw'),
    (7,  '金融', '台灣', 'finance_tw'),
    (8,  '企業', '台灣', 'enterprise_tw'),
    (9,  '廣告', '台灣', 'ads_tw'),
    (10, '其他', '台灣', 'other_tw')
) AS t(catsub_id, product_name, region, catsub_code)
