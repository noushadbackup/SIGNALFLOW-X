select {{ stg_common() }},
    {{ p('settlement_id') }} as settlement_id,
    {{ p('invoice_id') }} as invoice_id,
    {{ p('dealer_id') }} as dealer_id,
    {{ p('amount_bdt', 'numeric(14,2)') }} as amount_bdt
from {{ source('silver', 'events') }}
where event_type = 'mfg.dealer_settlement'
