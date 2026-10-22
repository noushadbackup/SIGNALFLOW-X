-- Grain: one shipment (shipment_id), pivoting DISPATCHED and DELIVERED status events.
select
    shipment_id,
    min(order_id) as order_id,
    min(warehouse_id) as warehouse_id,
    min(sku) as sku,
    min(quantity) as quantity,
    min(event_time) filter (where status = 'DISPATCHED') as dispatched_at,
    min(event_time) filter (where status = 'DELIVERED') as delivered_at,
    round({{ div(epoch_seconds_between("min(event_time) filter (where status = 'DISPATCHED')",
                                       "min(event_time) filter (where status = 'DELIVERED')"), '3600') }}, 1)
        as transit_hours,
    case when bool_or(status = 'DELIVERED') then 'DELIVERED' else 'IN_TRANSIT' end as current_status
from {{ ref('stg_mfg_shipment') }}
group by shipment_id
