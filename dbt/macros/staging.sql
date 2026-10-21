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
