select {{ stg_common() }},
    {{ p('movement_id') }} as movement_id,
    {{ p('warehouse_id') }} as warehouse_id,
    {{ p('sku') }} as sku,
    {{ p('quantity_delta', 'integer') }} as quantity_delta,
    {{ p('movement_type') }} as movement_type,
    {{ p('source_ref') }} as source_ref
from {{ source('silver', 'events') }}
where event_type = 'mfg.inventory_movement'
