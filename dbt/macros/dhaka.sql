{# Cross-database helpers. Each macro dispatches on the adapter (postgres in lite/stream, trino in the lake), so the
   models are written once and the lake-vs-lite differential test compares the same logic on two engines. #}

{# Business date in the Asia/Dhaka reporting timezone. #}
{% macro dhaka_date(ts) -%}{{ return(adapter.dispatch('dhaka_date', 'signalflow')(ts)) }}{%- endmacro %}
{% macro default__dhaka_date(ts) -%}(({{ ts }}) at time zone 'Asia/Dhaka')::date{%- endmacro %}
{% macro trino__dhaka_date(ts) -%}cast(({{ ts }}) at time zone 'Asia/Dhaka' as date){%- endmacro %}

{# First instant after a Dhaka business day, as a timestamp with time zone. #}
{% macro dhaka_day_end(d) -%}{{ return(adapter.dispatch('dhaka_day_end', 'signalflow')(d)) }}{%- endmacro %}
{% macro default__dhaka_day_end(d) -%}((({{ d }}) + 1)::timestamp at time zone 'Asia/Dhaka'){%- endmacro %}
{% macro trino__dhaka_day_end(d) -%}
    with_timezone(cast(date_add('day', 1, {{ d }}) as timestamp(6)), 'Asia/Dhaka')
{%- endmacro %}

{# A date plus n days. #}
{% macro add_days(d, n) -%}{{ return(adapter.dispatch('add_days', 'signalflow')(d, n)) }}{%- endmacro %}
{% macro default__add_days(d, n) -%}(({{ d }}) + {{ n }}){%- endmacro %}
{% macro trino__add_days(d, n) -%}date_add('day', {{ n }}, {{ d }}){%- endmacro %}

{# ISO day of week, Monday = 1 ... Sunday = 7, and the short English day name. #}
{% macro iso_dow(d) -%}{{ return(adapter.dispatch('iso_dow', 'signalflow')(d)) }}{%- endmacro %}
{% macro default__iso_dow(d) -%}extract(isodow from {{ d }})::int{%- endmacro %}
{% macro trino__iso_dow(d) -%}day_of_week({{ d }}){%- endmacro %}
{% macro day_name(d) -%}{{ return(adapter.dispatch('day_name', 'signalflow')(d)) }}{%- endmacro %}
{% macro default__day_name(d) -%}to_char({{ d }}, 'Dy'){%- endmacro %}
{% macro trino__day_name(d) -%}date_format({{ d }}, '%a'){%- endmacro %}

{# One row per day from first to last (inclusive), as a FROM-clause item exposing column d. #}
{% macro day_series(first, last) -%}{{ return(adapter.dispatch('day_series', 'signalflow')(first, last)) }}{%- endmacro %}
{% macro default__day_series(first, last) -%}
    generate_series({{ first }}, {{ last }}, interval '1 day') as g(d)
{%- endmacro %}
{% macro trino__day_series(first, last) -%}
    unnest(sequence({{ first }}, {{ last }}, interval '1' day)) as g(d)
{%- endmacro %}

{# Seconds between two timestamps as an exact decimal (millisecond inputs). #}
{% macro epoch_seconds_between(a, b) -%}
    {{ return(adapter.dispatch('epoch_seconds_between', 'signalflow')(a, b)) }}
{%- endmacro %}
{% macro default__epoch_seconds_between(a, b) -%}extract(epoch from ({{ b }}) - ({{ a }})){%- endmacro %}
{% macro trino__epoch_seconds_between(a, b) -%}
    (cast(date_diff('millisecond', {{ a }}, {{ b }}) as decimal(20, 3)) / 1000)
{%- endmacro %}

{# Exact decimal division. Postgres numeric keeps enough digits on its own; Trino's decimal division keeps only
   max(scale of the operands), so the numerator is widened to 12 decimals before dividing. Round the result. #}
{% macro div(num, den) -%}{{ return(adapter.dispatch('div', 'signalflow')(num, den)) }}{%- endmacro %}
{% macro default__div(num, den) -%}(({{ num }})::numeric / ({{ den }})){%- endmacro %}
{% macro trino__div(num, den) -%}(cast({{ num }} as decimal(38, 12)) / ({{ den }})){%- endmacro %}

{# Lower-case hex MD5 of a text expression. #}
{% macro md5_hex(expr) -%}{{ return(adapter.dispatch('md5_hex', 'signalflow')(expr)) }}{%- endmacro %}
{% macro default__md5_hex(expr) -%}md5({{ expr }}){%- endmacro %}
{% macro trino__md5_hex(expr) -%}lower(to_hex(md5(to_utf8({{ expr }})))){%- endmacro %}

{# Open end of an SCD2 interval. A real timestamp rather than Postgres 'infinity', which Trino cannot represent. #}
{% macro open_end_ts() -%}{{ return(adapter.dispatch('open_end_ts', 'signalflow')()) }}{%- endmacro %}
{% macro default__open_end_ts() -%}timestamptz '9999-12-31 00:00:00+00'{%- endmacro %}
{% macro trino__open_end_ts() -%}timestamp '9999-12-31 00:00:00.000000 UTC'{%- endmacro %}
