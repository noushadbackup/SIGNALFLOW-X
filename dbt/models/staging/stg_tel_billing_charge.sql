select {{ stg_common() }},
    {{ p('charge_id') }} as charge_id,
    {{ p('recharge_id') }} as recharge_id,
    {{ p('subscriber_id') }} as subscriber_id,
    {{ p('amount_bdt', 'numeric(14,2)') }} as amount_bdt,
    {{ p('charge_type') }} as charge_type
from {{ source('silver', 'events') }}
where event_type = 'telecom.billing_charge'
