-- kpi_status must agree with the recorded watermark and restatement log for every mart row.
with m as (
    select 'telecom' as stream, business_date, kpi_status from {{ ref('mart_telecom_revenue_daily') }}
    union all
    select 'telecom', business_date, kpi_status from {{ ref('mart_telecom_usage_daily') }}
    union all
    select 'mfg', business_date, kpi_status from {{ ref('mart_mfg_sales_daily') }}
),
expected as (
    select m.*,
        case
            when r.business_date is not null then 'RESTATED'
            when s.watermark >= {{ dhaka_day_end('m.business_date') }} then 'FINAL'
            else 'PROVISIONAL'
        end as expected_status
    from m
    left join {{ source('ops', 'kpi_restatements') }} r on r.stream = m.stream and r.business_date = m.business_date
    left join {{ source('ops', 'stream_state') }} s on s.stream = m.stream
)
select * from expected where kpi_status <> expected_status
