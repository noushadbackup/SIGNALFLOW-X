{# Use the configured schema verbatim (silver, gold) instead of dbt's default <target>_<custom> naming. In the lake,
   an optional namespace prefix (var lake_prefix) isolates parallel test lakes inside one Iceberg catalog. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set prefix = var('lake_prefix', '') if target.type == 'trino' else '' -%}
    {{ prefix ~ ((custom_schema_name or target.schema) | trim) }}
{%- endmacro %}
