select {{ stg_common() }},
    {{ p('subscriber_id') }} as subscriber_id,
    {{ p('action') }} as action,
    {{ p('segment') }} as segment,
    {{ p('home_region') }} as home_region,
    {{ p('plan_type') }} as plan_type
from {{ source('silver', 'events') }}
where event_type = 'telecom.subscriber_lifecycle'
