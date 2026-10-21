-- Grain: business_date x serving_region x usage_type.
select
    business_date, serving_region, usage_type,
    count(*) as usage_records,
    count(distinct subscriber_id) as distinct_subscribers,
    sum(quantity) as total_quantity,
    min(unit) as unit,
    sum(rated_amount_bdt) as rated_bdt,
    {{ kpi_status('telecom', 'u.business_date') }} as kpi_status
from {{ ref('fct_usage') }} u
group by business_date, serving_region, usage_type
