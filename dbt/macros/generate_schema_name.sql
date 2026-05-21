{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
      mysql_cache target writes through Trino's MySQL catalog connector.
      The target schema (lakehouse_cache) IS the MySQL database name;
      ignoring custom_schema_name ensures every model lands in the correct
      MySQL database regardless of its Iceberg schema override.
    #}
    {%- if target.name == 'mysql_cache' -%}
        {{ target.schema | trim }}
    {%- elif custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema | trim }}
    {%- endif -%}
{%- endmacro %}
