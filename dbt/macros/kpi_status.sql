{# KPI finality per ADR-0004. RESTATED: a beyond-watermark event changed this day after it was final.
   Pass a table-qualified date column: an unqualified name would bind to the subquery's own business_date. #}
{% macro kpi_status(stream, date_col) -%}
    case
        when exists (select 1 from {{ source('ops', 'kpi_restatements') }} r
                     where r.stream = '{{ stream }}' and r.business_date = {{ date_col }}) then 'RESTATED'
        when (select watermark from {{ source('ops', 'stream_state') }} where stream = '{{ stream }}')
             >= {{ dhaka_day_end(date_col) }} then 'FINAL'
        else 'PROVISIONAL'
    end
{%- endmacro %}
