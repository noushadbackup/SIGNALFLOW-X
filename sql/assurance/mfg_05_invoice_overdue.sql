-- MFG-05  Dealer invoice remains unmatched beyond its expected reconciliation window: no settlement at all after
-- the end of the due date (Asia/Dhaka). Partial payments are MFG-02's concern.
-- Subject: invoice_id. Anchor: end of the due date. Exposure: the invoice amount (overdue receivable).
-- Uses the stream clock (the as_of parameter): an invoice that is not yet due is not a discrepancy.
with invoice as (
    select payload ->> 'invoice_id' as invoice_id, payload ->> 'dealer_id' as dealer_id,
           payload ->> 'shipment_id' as shipment_id, (payload ->> 'amount_bdt')::numeric as amount_bdt,
           (payload ->> 'due_date')::date as due_date, event_id, event_time
    from silver.events
    where event_type = 'mfg.invoice'
),
settled as (
    select distinct payload ->> 'invoice_id' as invoice_id from silver.events
    where event_type = 'mfg.dealer_settlement'
)
select
    i.invoice_id                                            as subject_key,
    (i.due_date + 1)::timestamp at time zone 'Asia/Dhaka'   as anchor_time,
    (i.event_time at time zone 'Asia/Dhaka')::date          as business_date,
    'Dealers ' || coalesce(d.region_code, 'UNKNOWN')        as business_unit,
    d.region_code                                           as region_code,
    i.amount_bdt                                            as exposure_bdt,
    'Invoice ' || i.invoice_id || ' for BDT ' || to_char(i.amount_bdt, 'FM999,999,990.00') || ' to '
        || i.dealer_id || ' was due ' || to_char(i.due_date, 'YYYY-MM-DD') || ' and has no settlement' as summary,
    jsonb_build_object(
        'transactions', jsonb_build_array(jsonb_build_object(
            'role', 'invoice', 'event_type', 'mfg.invoice', 'id', i.invoice_id, 'event_id', i.event_id,
            'event_time', i.event_time, 'amount_bdt', i.amount_bdt, 'due_date', i.due_date,
            'shipment_id', i.shipment_id)),
        'facts', jsonb_build_object('dealer_id', i.dealer_id, 'credit_days', d.credit_days,
                                    'settlements_found', 0)) as evidence
from invoice i
left join ref.dealers d on d.dealer_id = i.dealer_id
where (i.due_date + 1)::timestamp at time zone 'Asia/Dhaka' <= %(as_of)s
  and not exists (select 1 from settled s where s.invoice_id = i.invoice_id)
