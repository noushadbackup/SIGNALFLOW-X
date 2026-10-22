-- TEL-01  Recharge succeeded but the corresponding billing record is absent.
-- Subject: recharge_id. Anchor: recharge event time. Exposure: the recharge amount (revenue credited to the
-- subscriber but not booked by billing). A billing record that arrives late clears the case automatically.
with recharge as (
    select event_id, event_time, payload ->> 'recharge_id' as recharge_id, payload ->> 'channel' as channel,
           payload ->> 'region' as region, (payload ->> 'amount_bdt')::numeric as amount_bdt,
           payload ->> 'subscriber_id' as subscriber_id, payload ->> 'payment_txn_ref' as payment_txn_ref
    from silver.events
    where event_type = 'telecom.recharge' and payload ->> 'status' = 'SUCCESS'
),
billed as (
    select distinct payload ->> 'recharge_id' as recharge_id
    from silver.events
    where event_type = 'telecom.billing_charge'
)
select
    r.recharge_id                                        as subject_key,
    r.event_time                                         as anchor_time,
    (r.event_time at time zone 'Asia/Dhaka')::date       as business_date,
    'Channel ' || r.channel                              as business_unit,
    r.region                                             as region_code,
    r.amount_bdt                                         as exposure_bdt,
    'Successful recharge of BDT ' || to_char(r.amount_bdt, 'FM999,999,990.00') || ' via ' || r.channel
        || ' has no billing record'                      as summary,
    jsonb_build_object(
        'transactions', jsonb_build_array(jsonb_build_object(
            'role', 'recharge', 'event_type', 'telecom.recharge', 'id', r.recharge_id, 'event_id', r.event_id,
            'event_time', r.event_time, 'amount_bdt', r.amount_bdt, 'status', 'SUCCESS',
            'subscriber_id', r.subscriber_id, 'payment_txn_ref', r.payment_txn_ref)),
        'facts', jsonb_build_object('billing_records_found', 0)) as evidence
from recharge r
where not exists (select 1 from billed b where b.recharge_id = r.recharge_id)
