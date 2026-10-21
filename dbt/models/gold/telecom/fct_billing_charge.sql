-- Grain: one balance credit (charge_id).
select charge_id, event_id, event_time, business_date, recharge_id, subscriber_id, amount_bdt, charge_type,
       arrival_class
from {{ ref('stg_tel_billing_charge') }}
