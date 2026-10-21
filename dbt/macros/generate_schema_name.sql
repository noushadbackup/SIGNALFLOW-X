{# Use the configured schema verbatim (silver, gold) instead of dbt's default <target>_<custom> naming. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
