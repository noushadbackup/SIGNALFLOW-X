-- Grain: one settlement per channel per business date (settlement_id). Expected values are from Silver
-- successful recharges for the same channel and business date.
with expected as (
    select channel, business_date, count(*) as expected_count, sum(amount_bdt) as expected_amount_bdt
    from {{ ref('stg_tel_recharge') }}
    where status = 'SUCCESS'
    group by channel, business_date
)
select
    s.settlement_id, s.event_id, s.event_time as settled_at, s.channel, s.settled_business_date as business_date,
    s.transaction_count, s.settled_amount_bdt,
    coalesce(e.expected_count, 0) as expected_count,
    coalesce(e.expected_amount_bdt, 0) as expected_amount_bdt,
    s.settled_amount_bdt - coalesce(e.expected_amount_bdt, 0) as difference_bdt
from {{ ref('stg_tel_channel_settlement') }} s
left join expected e on e.channel = s.channel and e.business_date = s.settled_business_date
