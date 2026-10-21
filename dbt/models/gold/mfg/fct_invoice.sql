-- Grain: one invoice (invoice_id) with settlement position as of the latest load.
with settled as (
    select invoice_id, count(*) as settlement_count, sum(amount_bdt) as settled_amount_bdt,
           max(event_time) as last_settled_at
    from {{ ref('stg_mfg_dealer_settlement') }}
    group by invoice_id
)
select
    i.invoice_id, i.event_id, i.event_time as invoiced_at, i.business_date, i.shipment_id, i.order_id, i.dealer_id,
    i.amount_bdt, i.due_date,
    coalesce(s.settlement_count, 0) as settlement_count,
    coalesce(s.settled_amount_bdt, 0) as settled_amount_bdt,
    i.amount_bdt - coalesce(s.settled_amount_bdt, 0) as outstanding_bdt,
    s.last_settled_at,
    case
        when s.settled_amount_bdt is null then 'UNPAID'
        when s.settled_amount_bdt < i.amount_bdt then 'PARTIALLY_PAID'
        when s.settled_amount_bdt = i.amount_bdt then 'PAID'
        else 'OVERPAID'
    end as settlement_status
from {{ ref('stg_mfg_invoice') }} i
left join settled s on s.invoice_id = i.invoice_id
