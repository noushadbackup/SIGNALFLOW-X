{# Typed field from the Silver payload (jsonb in Postgres, JSON text in the lake). Types are written the Postgres
   way (text, integer, date, numeric(p,s)) and translated for Trino. #}
{% macro p(field, type='text') -%}{{ return(adapter.dispatch('p', 'signalflow')(field, type)) }}{%- endmacro %}
{% macro default__p(field, type) -%}(payload ->> '{{ field }}')::{{ type }}{%- endmacro %}
{% macro trino__p(field, type) -%}
    {%- set value = "json_extract_scalar(payload, '$." ~ field ~ "')" -%}
    {%- if type == 'text' -%}{{ value }}
    {%- else -%}cast({{ value }} as {{ type | replace('numeric', 'decimal') }})
    {%- endif -%}
{%- endmacro %}

{# Common Silver columns every staging model exposes, for lineage back to Bronze and event-time analysis. #}
{% macro stg_common() -%}
    event_id,
    event_time,
    {{ dhaka_date('event_time') }} as business_date,
    schema_version,
    arrival_class,
    lateness_seconds,
    bronze_id
{%- endmacro %}
