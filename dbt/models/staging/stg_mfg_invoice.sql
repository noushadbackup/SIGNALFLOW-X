select {{ stg_common() }},
    {{ p('invoice_id') }} as invoice_id,
    {{ p('shipment_id') }} as shipment_id,
    {{ p('order_id') }} as order_id,
    {{ p('dealer_id') }} as dealer_id,
    {{ p('amount_bdt', 'numeric(14,2)') }} as amount_bdt,
    {{ p('due_date', 'date') }} as due_date
from {{ source('silver', 'events') }}
where event_type = 'mfg.invoice'
