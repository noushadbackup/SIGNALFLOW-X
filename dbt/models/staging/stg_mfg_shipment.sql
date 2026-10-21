select {{ stg_common() }},
    {{ p('shipment_id') }} as shipment_id,
    {{ p('order_id') }} as order_id,
    {{ p('warehouse_id') }} as warehouse_id,
    {{ p('sku') }} as sku,
    {{ p('quantity', 'integer') }} as quantity,
    {{ p('status') }} as status
from {{ source('silver', 'events') }}
where event_type = 'mfg.shipment'
