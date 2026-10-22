-- TEL-05  Delayed transactions create temporary discrepancies: a billing charge whose recharge is not (yet) a
-- successful recharge in Silver. In normal operation this only happens while the recharge event is still in
-- transit, so these cases are expected to clear on their own; one that survives the late-data horizon means
-- billing charged for a recharge that never succeeded.
-- Subject: charge_id. Anchor: billing event time. Exposure: the charged amount.
with charge as (
    select event_id, event_time, payload ->> 'charge_id' as charge_id, payload ->> 'recharge_id' as recharge_id,
           (payload ->> 'amount_bdt')::numeric as amount_bdt, payload ->> 'subscriber_id' as subscriber_id
    from silver.events
    where event_type = 'telecom.billing_charge'
),
recharge as (
    select payload ->> 'recharge_id' as recharge_id, payload ->> 'status' as status,
           payload ->> 'channel' as channel, payload ->> 'region' as region, event_id, event_time
    from silver.events
    where event_type = 'telecom.recharge'
)
select
    c.charge_id                                             as subject_key,
    c.event_time                                            as anchor_time,
    (c.event_time at time zone 'Asia/Dhaka')::date          as business_date,
    'Channel ' || coalesce(r.channel, 'UNKNOWN')            as business_unit,
    r.region                                                as region_code,
    c.amount_bdt                                            as exposure_bdt,
    'Billing charge of BDT ' || to_char(c.amount_bdt, 'FM999,999,990.00') || ' for recharge ' || c.recharge_id
        || case when r.recharge_id is null then ', which Silver has not received'
                else ' with status ' || r.status end        as summary,
    jsonb_build_object(
        'transactions', jsonb_build_array(jsonb_build_object(
            'role', 'billing_charge', 'event_type', 'telecom.billing_charge', 'id', c.charge_id,
            'event_id', c.event_id, 'event_time', c.event_time, 'amount_bdt', c.amount_bdt,
            'subscriber_id', c.subscriber_id, 'recharge_id', c.recharge_id))
            || case when r.recharge_id is null then '[]'::jsonb else jsonb_build_array(jsonb_build_object(
            'role', 'recharge', 'event_type', 'telecom.recharge', 'id', r.recharge_id, 'event_id', r.event_id,
            'event_time', r.event_time, 'status', r.status)) end,
        'facts', jsonb_build_object('recharge_received', r.recharge_id is not null)) as evidence
from charge c
left join recharge r on r.recharge_id = c.recharge_id
where r.recharge_id is null or r.status <> 'SUCCESS'
