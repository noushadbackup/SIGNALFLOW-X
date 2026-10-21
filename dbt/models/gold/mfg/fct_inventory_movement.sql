-- Grain: one inventory movement (movement_id). Adjustments are checked against approved stock counts.
select
    m.movement_id, m.event_id, m.event_time, m.business_date, m.warehouse_id, m.sku, p.category,
    m.quantity_delta, m.movement_type, m.source_ref,
    case
        when m.movement_type <> 'ADJUSTMENT' then 'NOT_REQUIRED'
        when a.approval_id is not null then 'AUTHORIZED'
        else 'NO_APPROVAL_FOUND'   -- as of the latest load; may still arrive late
    end as authorization_status,
    m.arrival_class
from {{ ref('stg_mfg_inventory_movement') }} m
left join {{ ref('dim_product') }} p on p.sku = m.sku
left join {{ ref('stg_mfg_stock_count_approval') }} a
    on m.movement_type = 'ADJUSTMENT' and a.approval_id = m.source_ref
