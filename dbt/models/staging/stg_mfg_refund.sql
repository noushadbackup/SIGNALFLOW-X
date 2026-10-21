select {{ stg_common() }},
    {{ p('refund_id') }} as refund_id,
    {{ p('return_id') }} as return_id,
    {{ p('dealer_id') }} as dealer_id,
    {{ p('amount_bdt', 'numeric(14,2)') }} as amount_bdt
from {{ source('silver', 'events') }}
where event_type = 'mfg.refund'
