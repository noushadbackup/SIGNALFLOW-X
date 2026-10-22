-- MFG-03  Return was refunded twice: more than one refund for a return, or refunds above the return's refund
-- amount. Subject: return_id. Anchor: event time of the second refund (or of the refund that exceeded the
-- amount). Exposure: refunded minus the return's refund amount.
with refund as (
    select payload ->> 'return_id' as return_id, payload ->> 'refund_id' as refund_id,
           payload ->> 'dealer_id' as dealer_id, (payload ->> 'amount_bdt')::numeric as amount_bdt,
           event_id, event_time
    from silver.events
    where event_type = 'mfg.refund'
),
per_return as (
    select return_id, count(distinct refund_id) as refunds, sum(amount_bdt) as refunded_bdt,
           min(dealer_id) as dealer_id, min(event_time) as first_refund_at,
           coalesce((array_agg(event_time order by event_time, refund_id))[2], max(event_time)) as anchor_time,
           (array_agg(amount_bdt order by event_time, refund_id))[1] as first_refund_bdt,
           jsonb_agg(jsonb_build_object(
               'role', 'refund', 'event_type', 'mfg.refund', 'id', refund_id, 'event_id', event_id,
               'event_time', event_time, 'amount_bdt', amount_bdt) order by event_time, refund_id) as transactions
    from refund
    group by return_id
),
returned as (
    select payload ->> 'return_id' as return_id, (payload ->> 'refund_amount_bdt')::numeric as refund_amount_bdt,
           payload ->> 'invoice_id' as invoice_id, event_id, event_time
    from silver.events
    where event_type = 'mfg.product_return'
)
select
    p.return_id                                             as subject_key,
    p.anchor_time                                           as anchor_time,
    (p.first_refund_at at time zone 'Asia/Dhaka')::date     as business_date,
    'Dealers ' || coalesce(d.region_code, 'UNKNOWN')        as business_unit,
    d.region_code                                           as region_code,
    p.refunded_bdt - coalesce(r.refund_amount_bdt, p.first_refund_bdt) as exposure_bdt,
    'Return ' || p.return_id || ' was refunded ' || p.refunds || ' times, BDT '
        || to_char(p.refunded_bdt, 'FM999,999,990.00') || ' against BDT '
        || to_char(coalesce(r.refund_amount_bdt, p.first_refund_bdt), 'FM999,999,990.00') || ' due' as summary,
    jsonb_build_object(
        'transactions', case when r.return_id is null then '[]'::jsonb else jsonb_build_array(jsonb_build_object(
            'role', 'return', 'event_type', 'mfg.product_return', 'id', r.return_id, 'event_id', r.event_id,
            'event_time', r.event_time, 'amount_bdt', r.refund_amount_bdt, 'invoice_id', r.invoice_id)) end
            || p.transactions,
        'facts', jsonb_build_object('refunds', p.refunds, 'refunded_bdt', p.refunded_bdt,
                                    'due_bdt', coalesce(r.refund_amount_bdt, p.first_refund_bdt),
                                    'dealer_id', p.dealer_id)) as evidence
from per_return p
left join returned r on r.return_id = p.return_id
left join ref.dealers d on d.dealer_id = p.dealer_id
where p.refunds > 1 or p.refunded_bdt > coalesce(r.refund_amount_bdt, p.refunded_bdt)
