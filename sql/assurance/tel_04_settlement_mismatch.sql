-- TEL-04  Channel settlement differs from expected revenue: the settled amount (or transaction count) for a
-- channel and business date is not equal to the successful recharges Silver holds for that channel and date.
-- Recharges that arrive late make the gap temporary; it closes when they arrive.
-- Subject: settlement_id. Anchor: settlement event time. Exposure: expected minus settled when positive.
with expected as (
    select payload ->> 'channel' as channel, (event_time at time zone 'Asia/Dhaka')::date as business_date,
           count(*) as recharges, sum((payload ->> 'amount_bdt')::numeric) as amount_bdt
    from silver.events
    where event_type = 'telecom.recharge' and payload ->> 'status' = 'SUCCESS'
    group by 1, 2
),
settlement as (
    select event_id, event_time, payload ->> 'settlement_id' as settlement_id, payload ->> 'channel' as channel,
           (payload ->> 'business_date')::date as business_date,
           (payload ->> 'transaction_count')::integer as transaction_count,
           (payload ->> 'settled_amount_bdt')::numeric as settled_bdt
    from silver.events
    where event_type = 'telecom.channel_settlement'
)
select
    s.settlement_id                                         as subject_key,
    s.event_time                                            as anchor_time,
    s.business_date                                         as business_date,
    'Channel ' || s.channel                                 as business_unit,
    null::text                                              as region_code,
    greatest(coalesce(e.amount_bdt, 0) - s.settled_bdt, 0)  as exposure_bdt,
    s.channel || ' settled BDT ' || to_char(s.settled_bdt, 'FM999,999,990.00') || ' for '
        || to_char(s.business_date, 'YYYY-MM-DD') || '; successful recharges in Silver total BDT '
        || to_char(coalesce(e.amount_bdt, 0), 'FM999,999,990.00') as summary,
    jsonb_build_object(
        'transactions', jsonb_build_array(jsonb_build_object(
            'role', 'settlement', 'event_type', 'telecom.channel_settlement', 'id', s.settlement_id,
            'event_id', s.event_id, 'event_time', s.event_time, 'amount_bdt', s.settled_bdt)),
        'facts', jsonb_build_object(
            'settled_bdt', s.settled_bdt, 'expected_bdt', coalesce(e.amount_bdt, 0),
            'difference_bdt', s.settled_bdt - coalesce(e.amount_bdt, 0),
            'settled_count', s.transaction_count, 'expected_count', coalesce(e.recharges, 0))) as evidence
from settlement s
left join expected e on e.channel = s.channel and e.business_date = s.business_date
where s.settled_bdt <> coalesce(e.amount_bdt, 0) or s.transaction_count <> coalesce(e.recharges, 0)
