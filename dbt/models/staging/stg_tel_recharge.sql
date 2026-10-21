-- v1.0 and v1.1 are compatible; promo_code is null for v1.0 events.
select {{ stg_common() }},
    {{ p('recharge_id') }} as recharge_id,
    {{ p('subscriber_id') }} as subscriber_id,
    {{ p('amount_bdt', 'numeric(14,2)') }} as amount_bdt,
    {{ p('channel') }} as channel,
    {{ p('payment_txn_ref') }} as payment_txn_ref,
    {{ p('status') }} as status,
    {{ p('failure_reason') }} as failure_reason,
    {{ p('region') }} as region,
    {{ p('promo_code') }} as promo_code
from {{ source('silver', 'events') }}
where event_type = 'telecom.recharge'
