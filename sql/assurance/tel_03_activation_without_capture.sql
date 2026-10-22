-- TEL-03  Package activation succeeded but payment capture failed (or no capture was recorded).
-- Subject: activation_id. Anchor: activation event time. Exposure: the package price (service delivered,
-- payment not collected). A CAPTURED record for the same payment_txn_ref clears the case.
with activation as (
    select event_id, event_time, payload ->> 'activation_id' as activation_id,
           payload ->> 'package_code' as package_code, (payload ->> 'price_bdt')::numeric as price_bdt,
           payload ->> 'payment_txn_ref' as payment_txn_ref, payload ->> 'subscriber_id' as subscriber_id
    from silver.events
    where event_type = 'telecom.package_activation' and payload ->> 'status' = 'ACTIVATED'
),
capture as (
    select event_id, event_time, payload ->> 'capture_id' as capture_id, payload ->> 'status' as status,
           payload ->> 'payment_txn_ref' as payment_txn_ref, (payload ->> 'amount_bdt')::numeric as amount_bdt
    from silver.events
    where event_type = 'telecom.payment_capture'
)
select
    a.activation_id                                         as subject_key,
    a.event_time                                            as anchor_time,
    (a.event_time at time zone 'Asia/Dhaka')::date          as business_date,
    'Bundles ' || a.package_code                            as business_unit,
    null::text                                              as region_code,
    a.price_bdt                                             as exposure_bdt,
    'Bundle ' || a.package_code || ' activated for BDT ' || to_char(a.price_bdt, 'FM999,999,990.00') || ' but '
        || case when count(c.capture_id) = 0 then 'no payment capture was recorded'
                else 'payment capture ' || string_agg(c.status, ', ' order by c.event_time) end as summary,
    jsonb_build_object(
        'transactions', jsonb_build_array(jsonb_build_object(
            'role', 'activation', 'event_type', 'telecom.package_activation', 'id', a.activation_id,
            'event_id', a.event_id, 'event_time', a.event_time, 'amount_bdt', a.price_bdt, 'status', 'ACTIVATED',
            'subscriber_id', a.subscriber_id, 'payment_txn_ref', a.payment_txn_ref))
            || coalesce(jsonb_agg(jsonb_build_object(
            'role', 'capture', 'event_type', 'telecom.payment_capture', 'id', c.capture_id, 'event_id', c.event_id,
            'event_time', c.event_time, 'amount_bdt', c.amount_bdt, 'status', c.status)
            order by c.event_time) filter (where c.capture_id is not null), '[]'::jsonb),
        'facts', jsonb_build_object('captures_found', count(c.capture_id))) as evidence
from activation a
left join capture c on c.payment_txn_ref = a.payment_txn_ref
group by a.event_id, a.event_time, a.activation_id, a.package_code, a.price_bdt, a.payment_txn_ref, a.subscriber_id
having count(c.capture_id) filter (where c.status = 'CAPTURED') = 0
