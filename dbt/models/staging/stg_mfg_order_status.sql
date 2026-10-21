select {{ stg_common() }},
    {{ p('order_id') }} as order_id,
    {{ p('status') }} as status,
    {{ p('reason') }} as reason
from {{ source('silver', 'events') }}
where event_type = 'mfg.order_status'
