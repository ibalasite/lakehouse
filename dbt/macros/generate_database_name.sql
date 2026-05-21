{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {#
      For mysql_cache target: cache models write to MySQL, but all refs
      (gold/silver/bronze) must still resolve to the Iceberg catalog.
      We detect cache models by their configured schema value.
    #}
    {%- if target.name == 'mysql_cache' -%}
        {%- set raw_schema = node.config.get('schema') if node else none -%}
        {%- if raw_schema == 'cache' -%}
            {{ target.database | trim }}
        {%- else -%}
            iceberg
        {%- endif -%}
    {%- else -%}
        {{ target.database | trim }}
    {%- endif -%}
{%- endmacro %}
