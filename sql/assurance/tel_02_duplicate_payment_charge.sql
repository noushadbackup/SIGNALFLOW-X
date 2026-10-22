-- TEL-02  One payment was charged twice: two or more successful recharges carry the same payment_txn_ref.
-- Technical redeliveries (same event_id) never reach Silver, so every row here is a distinct business recharge.
-- Subject: payment_txn_ref. Anchor: event time of the second charge. Exposure: every charge after the first
-- (an over-charge the subscriber can claim back).
with charge as (
    select event_id, event_time, payload ->> 'recharge_id' as recharge_id, payload ->> 'channel' as channel,
           payload ->> 'region' as region, (payload ->> 'amount_bdt')::numeric as amount_bdt,
           payload ->> 'subscriber_id' as subscriber_id, payload ->> 'payment_txn_ref' as payment_txn_ref
    from silver.events
    where event_type = 'telecom.recharge' and payload ->> 'status' = 'SUCCESS'
),
grouped as (
    select payment_txn_ref,
           count(distinct recharge_id)                                  as charges,
           sum(amount_bdt)                                              as charged_bdt,
           (array_agg(amount_bdt order by event_time, recharge_id))[1]  as first_amount_bdt,
           (array_agg(event_time order by event_time, recharge_id))[2]  as second_charge_at,
           min(event_time)                                              as first_charge_at,
           min(channel)                                                 as channel,
           min(region)                                                  as region,
           jsonb_agg(jsonb_build_object(
               'role', 'charge', 'event_type', 'telecom.recharge', 'id', recharge_id, 'event_id', event_id,
               'event_time', event_time, 'amount_bdt', amount_bdt, 'status', 'SUCCESS',
               'subscriber_id', subscriber_id) order by event_time, recharge_id) as transactions
    from charge
    group by payment_txn_ref
    having count(distinct recharge_id) > 1
)
select
    payment_txn_ref                                         as subject_key,
    second_charge_at                                        as anchor_time,
    (first_charge_at at time zone 'Asia/Dhaka')::date       as business_date,
    'Channel ' || channel                                   as business_unit,
    region                                                  as region_code,
    charged_bdt - first_amount_bdt                          as exposure_bdt,
    'Payment ' || payment_txn_ref || ' was charged ' || charges || ' times (BDT '
        || to_char(charged_bdt, 'FM999,999,990.00') || ' in total)' as summary,
    jsonb_build_object('transactions', transactions,
                       'facts', jsonb_build_object('charges', charges, 'charged_bdt', charged_bdt,
                                                   'expected_bdt', first_amount_bdt)) as evidence
from grouped
