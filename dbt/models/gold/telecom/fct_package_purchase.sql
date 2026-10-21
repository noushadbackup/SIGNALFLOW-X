-- Grain: one bundle activation decision (activation_id), with the gateway capture outcome for its payment.
select
    a.activation_id, a.event_id, a.event_time, a.business_date, a.subscriber_id, a.package_code, a.price_bdt,
    a.payment_txn_ref, a.status as activation_status,
    c.capture_id, c.status as capture_status, c.amount_bdt as captured_amount_bdt,
    a.arrival_class
from {{ ref('stg_tel_package_activation') }} a
left join {{ ref('stg_tel_payment_capture') }} c on c.payment_txn_ref = a.payment_txn_ref
