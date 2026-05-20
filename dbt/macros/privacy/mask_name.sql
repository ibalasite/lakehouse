{#
  mask_name(col)
  ─────────────────────────────────────────────────────────────────────────────
  Masks a customer name for PII compliance.

  Strategy: keep the first character (family name in CJK conventions), replace
  all remaining characters with '***'.  Examples:
    '王小明'  → '王***'
    'John'    → 'J***'
    NULL      → NULL (preserved so downstream IS NULL checks still work)

  col must be a bare column name string (e.g. 'prblm_name'), assembled at
  dbt compile time — not a runtime user value.
#}
{% macro mask_name(col) %}
  CASE
    WHEN {{ col }} IS NULL THEN NULL
    WHEN length({{ col }}) = 0 THEN ''
    ELSE concat(substr({{ col }}, 1, 1), '***')
  END
{% endmacro %}
