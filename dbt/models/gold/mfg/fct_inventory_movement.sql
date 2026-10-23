-- Grain: one inventory movement (movement_id). Adjustments are checked against approved stock counts: the approval
-- must exist, name the same warehouse and SKU, have counted minus system quantity equal to the delta, and not have
-- been applied already by an earlier movement (a replayed or duplicated adjustment).
with adjustment_use as (
    select movement_id,
           row_number() over (partition by source_ref order by event_time, movement_id) as use_no
    from {{ ref('stg_mfg_inventory_movement') }}
    where movement_type = 'ADJUSTMENT'
)
select
    m.movement_id, m.event_id, m.event_time, m.business_date, m.warehouse_id, m.sku, p.category,
    m.quantity_delta, m.movement_type, m.source_ref,
    case
        when m.movement_type <> 'ADJUSTMENT' then 'NOT_REQUIRED'
        when a.approval_id is null then 'NO_APPROVAL_FOUND'   -- as of the latest load; may still arrive late
        when a.warehouse_id <> m.warehouse_id or a.sku <> m.sku
             or a.counted_quantity - a.system_quantity <> m.quantity_delta then 'APPROVAL_MISMATCH'
        when u.use_no > 1 then 'APPROVAL_REUSED'
        else 'AUTHORIZED'
    end as authorization_status,
    m.arrival_class
from {{ ref('stg_mfg_inventory_movement') }} m
left join {{ ref('dim_product') }} p on p.sku = m.sku
left join adjustment_use u on u.movement_id = m.movement_id
left join {{ ref('stg_mfg_stock_count_approval') }} a
    on m.movement_type = 'ADJUSTMENT' and a.approval_id = m.source_ref
