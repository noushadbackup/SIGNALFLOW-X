select {{ stg_common() }},
    {{ p('settlement_id') }} as settlement_id,
    {{ p('channel') }} as channel,
    {{ p('business_date', 'date') }} as settled_business_date,
    {{ p('transaction_count', 'integer') }} as transaction_count,
    {{ p('settled_amount_bdt', 'numeric(14,2)') }} as settled_amount_bdt
from {{ source('silver', 'events') }}
where event_type = 'telecom.channel_settlement'
