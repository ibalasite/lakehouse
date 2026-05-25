{#
  mysql_cache_materialized()
  ─────────────────────────────────────────────────────────────────────────────
  Returns 'incremental' for mysql_cache target, 'table' for prod (Iceberg).

  MySQL cache tables are pre-created in 03_mysql_init.sql with PARTITION BY
  RANGE. Using 'table' materialization on mysql_cache would cause dbt to
  DROP+CREATE the table, destroying partition structure and breaking
  rotate_mysql_partitions (error 1505). Incremental strategy 'append' with a
  pre-hook DELETE preserves the DDL while still doing a full data refresh.

  Target routing is isolated here per CLAUDE.md — no target.name in model SQL.
#}
{% macro mysql_cache_materialized() -%}
{{ 'incremental' if target.name == 'mysql_cache' else 'table' }}
{%- endmacro %}

