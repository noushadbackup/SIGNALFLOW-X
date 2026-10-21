select {{ stg_common() }},
    {{ p('batch_id') }} as batch_id,
    {{ p('factory_id') }} as factory_id,
    {{ p('sku') }} as sku,
    {{ p('quantity', 'integer') }} as quantity,
    {{ p('destination_warehouse_id') }} as destination_warehouse_id
from {{ source('silver', 'events') }}
where event_type = 'mfg.production_batch'
