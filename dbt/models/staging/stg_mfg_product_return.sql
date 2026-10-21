select {{ stg_common() }},
    {{ p('return_id') }} as return_id,
    {{ p('invoice_id') }} as invoice_id,
    {{ p('dealer_id') }} as dealer_id,
    {{ p('sku') }} as sku,
    {{ p('quantity', 'integer') }} as quantity,
    {{ p('refund_amount_bdt', 'numeric(14,2)') }} as refund_amount_bdt,
    {{ p('warehouse_id') }} as warehouse_id
from {{ source('silver', 'events') }}
where event_type = 'mfg.product_return'
