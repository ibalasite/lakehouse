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

{#
  mysql_cache_delete_hook(relation)
  ─────────────────────────────────────────────────────────────────────────────
  Pre-hook for mysql_cache incremental models: deletes all rows (preserving
  partition structure) before the append INSERT, making each run a full refresh.
  Returns a no-op SELECT for prod target so the hook is always syntactically
  valid.
#}
{% macro mysql_cache_delete_hook(relation) -%}
  {%- if target.name == 'mysql_cache' -%}
    DELETE FROM {{ relation }}
  {%- else -%}
    SELECT 1
  {%- endif %}
{%- endmacro %}
