-- Grain: one recharge attempt (recharge_id). Subscriber attributes are as-of the recharge event time.
with billing as (
    select recharge_id, count(*) as billing_records, sum(amount_bdt) as billed_amount_bdt
    from {{ ref('stg_tel_billing_charge') }}
    group by recharge_id
)
select
    r.recharge_id,
    r.event_id,
    r.event_time,
    r.business_date,
    r.subscriber_id,
    s.subscriber_sk,
    s.segment as segment_at_recharge,
    s.home_region as home_region_at_recharge,
    r.region,
    r.channel,
    r.status,
    r.failure_reason,
    r.status = 'SUCCESS' as is_success,
    r.amount_bdt,
    r.payment_txn_ref,
    r.promo_code,
    r.schema_version,
    r.arrival_class,
    coalesce(b.billing_records, 0) as billing_records,
    coalesce(b.billed_amount_bdt, 0) as billed_amount_bdt
from {{ ref('stg_tel_recharge') }} r
left join {{ ref('dim_subscriber_scd2') }} s
    on s.subscriber_id = r.subscriber_id and r.event_time >= s.valid_from and r.event_time < s.valid_to
left join billing b on b.recharge_id = r.recharge_id
