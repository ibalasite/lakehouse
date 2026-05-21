{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
      mysql_cache target: only cache models (configured schema='cache')
      are re-routed to lakehouse_cache. All other models (gold/silver/bronze)
      keep their original schema so that ref() resolves to iceberg.gold.*,
      iceberg.silver.*, etc. — not mysql.lakehouse_cache.*.
    #}
    {%- if target.name == 'mysql_cache' and custom_schema_name == 'cache' -%}
        {{ target.schema | trim }}
    {%- elif custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema | trim }}
    {%- endif -%}
{%- endmacro %}
