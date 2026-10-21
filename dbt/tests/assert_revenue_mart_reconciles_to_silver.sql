-- Gold revenue must equal successful recharges in Silver, per business date and channel, to the poisha.
with silver as (
    select business_date, channel, sum(amount_bdt) as amount
    from {{ ref('stg_tel_recharge') }} where status = 'SUCCESS' group by 1, 2
),
gold as (
    select business_date, channel, sum(gross_recharge_bdt) as amount
    from {{ ref('mart_telecom_revenue_daily') }} group by 1, 2
)
select coalesce(s.business_date, g.business_date) as business_date, coalesce(s.channel, g.channel) as channel,
       s.amount as silver_amount, g.amount as gold_amount
from silver s full outer join gold g on g.business_date = s.business_date and g.channel = s.channel
where s.amount is distinct from g.amount
