{#
  platform_iceberg_table_type()
  ─────────────────────────────────────────────────────────────────────────────
  Returns 'iceberg' for Iceberg targets (prod), none for MySQL hot-tier target.
  Used in cache model config() to ensure Iceberg format on the cold tier without
  passing an invalid table_type to Trino's MySQL connector catalog.

  Per CLAUDE.md: all target-conditional logic belongs in macros/platform/*, not
  in model SQL.
#}
{% macro platform_iceberg_table_type() -%}
  {%- if target.name == 'mysql_cache' -%}
    {{ none }}
  {%- else -%}
    iceberg
  {%- endif -%}
{%- endmacro %}


{#
  platform_merge_strategy()
  ─────────────────────────────────────────────────────────────────────────────
  Returns the incremental_strategy appropriate for the current target.
  Both Iceberg (Trino Iceberg connector) and MySQL hot-tier (Trino MySQL
  connector) use 'merge' — Trino's MERGE statement is connector-agnostic.

  EDD §10.5: cache models use platform_merge_strategy() so the strategy can be
  overridden per environment without touching model SQL.
#}
{% macro platform_merge_strategy() -%}
merge
{%- endmacro %}
