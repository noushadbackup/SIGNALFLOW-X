select {{ stg_common() }},
    {{ p('capture_id') }} as capture_id,
    {{ p('payment_txn_ref') }} as payment_txn_ref,
    {{ p('subscriber_id') }} as subscriber_id,
    {{ p('amount_bdt', 'numeric(14,2)') }} as amount_bdt,
    {{ p('status') }} as status,
    {{ p('purpose') }} as purpose
from {{ source('silver', 'events') }}
where event_type = 'telecom.payment_capture'
