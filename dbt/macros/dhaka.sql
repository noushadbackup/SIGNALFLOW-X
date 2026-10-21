{# Business date in the Asia/Dhaka reporting timezone. #}
{% macro dhaka_date(ts) -%}
    (({{ ts }}) at time zone 'Asia/Dhaka')::date
{%- endmacro %}

{# First instant after a Dhaka business day, as timestamptz. #}
{% macro dhaka_day_end(d) -%}
    ((({{ d }}) + 1)::timestamp at time zone 'Asia/Dhaka')
{%- endmacro %}

{# Typed field from the Silver payload. #}
{% macro p(field, type='text') -%}
    (payload ->> '{{ field }}')::{{ type }}
{%- endmacro %}
