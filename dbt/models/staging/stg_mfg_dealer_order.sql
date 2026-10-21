select {{ stg_common() }},
    {{ p('order_id') }} as order_id,
    {{ p('dealer_id') }} as dealer_id,
    {{ p('sku') }} as sku,
    {{ p('quantity', 'integer') }} as quantity,
    {{ p('unit_price_bdt', 'numeric(14,2)') }} as unit_price_bdt
from {{ source('silver', 'events') }}
where event_type = 'mfg.dealer_order'
