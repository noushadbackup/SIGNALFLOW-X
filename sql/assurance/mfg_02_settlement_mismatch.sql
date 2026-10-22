-- MFG-02  Sales transaction and settlement amounts disagree: the settlements received for an invoice do not add
-- up to the invoice amount (short or over payment), or a settlement names an invoice Silver has not received.
-- Subject: invoice_id. Anchor: latest settlement event time. Exposure: invoice minus settled when positive.
with settled as (
    select payload ->> 'invoice_id' as invoice_id, sum((payload ->> 'amount_bdt')::numeric) as settled_bdt,
           count(*) as settlements, max(event_time) as last_settled_at, min(payload ->> 'dealer_id') as dealer_id,
           jsonb_agg(jsonb_build_object(
               'role', 'settlement', 'event_type', 'mfg.dealer_settlement', 'id', payload ->> 'settlement_id',
               'event_id', event_id, 'event_time', event_time, 'amount_bdt', (payload ->> 'amount_bdt')::numeric)
               order by event_time) as transactions
    from silver.events
    where event_type = 'mfg.dealer_settlement'
    group by 1
),
invoice as (
    select payload ->> 'invoice_id' as invoice_id, (payload ->> 'amount_bdt')::numeric as amount_bdt,
           payload ->> 'due_date' as due_date, event_id, event_time
    from silver.events
    where event_type = 'mfg.invoice'
)
select
    s.invoice_id                                            as subject_key,
    s.last_settled_at                                       as anchor_time,
    (s.last_settled_at at time zone 'Asia/Dhaka')::date     as business_date,
    'Dealers ' || coalesce(d.region_code, 'UNKNOWN')        as business_unit,
    d.region_code                                           as region_code,
    greatest(coalesce(i.amount_bdt, 0) - s.settled_bdt, 0)  as exposure_bdt,
    case when i.invoice_id is null
         then 'Settlement of BDT ' || to_char(s.settled_bdt, 'FM999,999,990.00') || ' names invoice '
              || s.invoice_id || ', which Silver has not received'
         else 'Invoice ' || s.invoice_id || ' for BDT ' || to_char(i.amount_bdt, 'FM999,999,990.00')
              || ' was settled with BDT ' || to_char(s.settled_bdt, 'FM999,999,990.00') end as summary,
    jsonb_build_object(
        'transactions', case when i.invoice_id is null then '[]'::jsonb else jsonb_build_array(jsonb_build_object(
            'role', 'invoice', 'event_type', 'mfg.invoice', 'id', i.invoice_id, 'event_id', i.event_id,
            'event_time', i.event_time, 'amount_bdt', i.amount_bdt, 'due_date', i.due_date)) end || s.transactions,
        'facts', jsonb_build_object('invoice_bdt', i.amount_bdt, 'settled_bdt', s.settled_bdt,
                                    'difference_bdt', s.settled_bdt - i.amount_bdt, 'settlements', s.settlements,
                                    'dealer_id', s.dealer_id)) as evidence
from settled s
left join invoice i on i.invoice_id = s.invoice_id
left join ref.dealers d on d.dealer_id = s.dealer_id
where i.invoice_id is null or s.settled_bdt <> i.amount_bdt
