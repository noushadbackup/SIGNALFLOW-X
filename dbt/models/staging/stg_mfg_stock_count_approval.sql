select {{ stg_common() }},
    {{ p('approval_id') }} as approval_id,
    {{ p('warehouse_id') }} as warehouse_id,
    {{ p('sku') }} as sku,
    {{ p('counted_quantity', 'integer') }} as counted_quantity,
    {{ p('system_quantity', 'integer') }} as system_quantity,
    {{ p('approved_by_role') }} as approved_by_role
from {{ source('silver', 'events') }}
where event_type = 'mfg.stock_count_approval'
