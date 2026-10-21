select {{ stg_common() }},
    {{ p('usage_id') }} as usage_id,
    {{ p('subscriber_id') }} as subscriber_id,
    {{ p('usage_type') }} as usage_type,
    {{ p('quantity', 'numeric(14,2)') }} as quantity,
    {{ p('unit') }} as unit,
    {{ p('serving_region') }} as serving_region,
    {{ p('cell_area_code') }} as cell_area_code,
    {{ p('rated_amount_bdt', 'numeric(14,2)') }} as rated_amount_bdt
from {{ source('silver', 'events') }}
where event_type = 'telecom.usage_record'
