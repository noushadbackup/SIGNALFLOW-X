-- Grain: warehouse x SKU. On-hand is the sum of all accepted movements. Days of cover and stockout risk are
-- DERIVED estimates from trailing outbound demand, not forecasts.
with as_of as (
    select max(event_time) as as_of_time, min(event_time) as first_time
    from {{ ref('fct_inventory_movement') }}
),
demand_window as (
    -- days of history behind the trailing demand figure: at least 1, at most 7
    select as_of_time,
           greatest(least({{ div(epoch_seconds_between('first_time', 'as_of_time'), '86400') }}, 7), 1) as window_days
    from as_of
),
positions as (
    select
        m.warehouse_id, m.sku,
        sum(m.quantity_delta) as on_hand_qty,
        -sum(m.quantity_delta) filter (where m.movement_type = 'SHIPMENT_OUT'
                                       and m.event_time > a.as_of_time - interval '7' day) as outbound_7d_qty,
        count(*) filter (where m.authorization_status = 'NO_APPROVAL_FOUND') as unauthorized_adjustments,
        max(m.event_time) as last_movement_at
    from {{ ref('fct_inventory_movement') }} m cross join as_of a
    group by m.warehouse_id, m.sku
),
rated as (
    select p.*,
           {{ div('coalesce(p.outbound_7d_qty, 0)', 'w.window_days') }} as daily_outbound,
           case when coalesce(p.outbound_7d_qty, 0) = 0 then null
                else {{ div('p.on_hand_qty * w.window_days', 'p.outbound_7d_qty') }}
           end as cover_days,
           w.as_of_time
    from positions p cross join demand_window w
)
select
    r.warehouse_id, w.region_code, r.sku, pr.category,
    r.on_hand_qty,
    coalesce(r.outbound_7d_qty, 0) as outbound_7d_qty,
    round(r.daily_outbound, 2) as avg_daily_outbound,
    round(r.cover_days, 1) as days_of_cover,
    case
        when r.on_hand_qty <= 0 then 'OUT_OF_STOCK'
        when coalesce(r.outbound_7d_qty, 0) = 0 then 'NO_RECENT_DEMAND'
        when r.cover_days < 3 then 'HIGH'
        when r.cover_days < 7 then 'MEDIUM'
        else 'LOW'
    end as stockout_risk,
    r.unauthorized_adjustments,
    r.last_movement_at,
    r.as_of_time
from rated r
left join {{ ref('dim_warehouse') }} w on w.warehouse_id = r.warehouse_id
left join {{ ref('dim_product') }} pr on pr.sku = r.sku
