-- Grain: business_date x home_region x segment. Active base at the end of each Dhaka business day.
with days as (
    select date_day from {{ ref('dim_date') }}
    where date_day between (select min(business_date) from {{ ref('fct_recharge') }})
                       and (select max(business_date) from {{ ref('fct_recharge') }})
)
select
    d.date_day as business_date,
    s.home_region,
    s.segment,
    count(*) as active_subscribers,
    {{ kpi_status('telecom', 'd.date_day') }} as kpi_status
from days d
join {{ ref('dim_subscriber_scd2') }} s
    on s.valid_from < {{ dhaka_day_end('d.date_day') }} and s.valid_to >= {{ dhaka_day_end('d.date_day') }}
where s.status = 'ACTIVE'
group by d.date_day, s.home_region, s.segment
