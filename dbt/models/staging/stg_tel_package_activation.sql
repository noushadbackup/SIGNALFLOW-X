select {{ stg_common() }},
    {{ p('activation_id') }} as activation_id,
    {{ p('subscriber_id') }} as subscriber_id,
    {{ p('package_code') }} as package_code,
    {{ p('price_bdt', 'numeric(14,2)') }} as price_bdt,
    {{ p('payment_txn_ref') }} as payment_txn_ref,
    {{ p('status') }} as status
from {{ source('silver', 'events') }}
where event_type = 'telecom.package_activation'
