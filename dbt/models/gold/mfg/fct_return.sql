-- Grain: one return (return_id) with refunds paid against it.
with refunds as (
    select return_id, count(*) as refund_count, sum(amount_bdt) as refunded_bdt
    from {{ ref('stg_mfg_refund') }}
    group by return_id
)
select
    r.return_id, r.event_id, r.event_time as returned_at, r.business_date, r.invoice_id, r.dealer_id, r.sku,
    r.quantity, r.refund_amount_bdt as approved_refund_bdt, r.warehouse_id,
    coalesce(f.refund_count, 0) as refund_count,
    coalesce(f.refunded_bdt, 0) as refunded_bdt
from {{ ref('stg_mfg_product_return') }} r
left join refunds f on f.return_id = r.return_id
