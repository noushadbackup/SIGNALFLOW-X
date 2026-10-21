-- Grain: business_date x region (where the recharge happened) x channel.
select
    business_date,
    region,
    channel,
    count(*) as recharge_attempts,
    count(*) filter (where is_success) as successful_recharges,
    count(*) filter (where not is_success) as failed_recharges,
    round(count(*) filter (where is_success)::numeric / nullif(count(*), 0), 4) as success_rate,
    coalesce(sum(amount_bdt) filter (where is_success), 0) as gross_recharge_bdt,
    coalesce(sum(billed_amount_bdt) filter (where is_success), 0) as billed_bdt,
    count(*) filter (where is_success and billing_records = 0) as successful_without_billing,
    count(*) filter (where arrival_class <> 'ON_TIME') as late_arrivals,
    {{ kpi_status('telecom', 'f.business_date') }} as kpi_status
from {{ ref('fct_recharge') }} f
group by business_date, region, channel
