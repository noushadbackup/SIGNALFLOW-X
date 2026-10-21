-- Grain: warehouse x SKU. On-hand is the sum of all accepted movements. Days of cover and stockout risk are
-- DERIVED estimates from trailing outbound demand, not forecasts.
with as_of as (
    select max(event_time) as as_of_time, min(event_time) as first_time
    from {{ ref('fct_inventory_movement') }}
),
positions as (
    select
        m.warehouse_id, m.sku,
        sum(m.quantity_delta) as on_hand_qty,
        -sum(m.quantity_delta) filter (where m.movement_type = 'SHIPMENT_OUT'
                                       and m.event_time > a.as_of_time - interval '7 days') as outbound_7d_qty,
        count(*) filter (where m.authorization_status = 'NO_APPROVAL_FOUND') as unauthorized_adjustments,
        max(m.event_time) as last_movement_at
    from {{ ref('fct_inventory_movement') }} m cross join as_of a
    group by m.warehouse_id, m.sku
)
select
    p.warehouse_id, w.region_code, p.sku, pr.category,
    p.on_hand_qty,
    coalesce(p.outbound_7d_qty, 0) as outbound_7d_qty,
    round(coalesce(p.outbound_7d_qty, 0)
          / greatest(least(extract(epoch from a.as_of_time - a.first_time) / 86400.0, 7), 1), 2) as avg_daily_outbound,
    case when coalesce(p.outbound_7d_qty, 0) = 0 then null
         else round(p.on_hand_qty / (p.outbound_7d_qty
              / greatest(least(extract(epoch from a.as_of_time - a.first_time) / 86400.0, 7), 1)), 1)
    end as days_of_cover,
    case
        when p.on_hand_qty <= 0 then 'OUT_OF_STOCK'
        when coalesce(p.outbound_7d_qty, 0) = 0 then 'NO_RECENT_DEMAND'
        when p.on_hand_qty / (p.outbound_7d_qty
             / greatest(least(extract(epoch from a.as_of_time - a.first_time) / 86400.0, 7), 1)) < 3 then 'HIGH'
        when p.on_hand_qty / (p.outbound_7d_qty
             / greatest(least(extract(epoch from a.as_of_time - a.first_time) / 86400.0, 7), 1)) < 7 then 'MEDIUM'
        else 'LOW'
    end as stockout_risk,
    p.unauthorized_adjustments,
    p.last_movement_at,
    a.as_of_time
from positions p
cross join as_of a
left join {{ ref('dim_warehouse') }} w on w.warehouse_id = p.warehouse_id
left join {{ ref('dim_product') }} pr on pr.sku = p.sku
