-- MFG-04  Inventory adjustment has no authorized source event: an ADJUSTMENT movement whose source_ref is not an
-- approved stock count for the same warehouse and SKU with counted minus system quantity equal to the delta.
-- Subject: movement_id. Anchor: movement event time. Exposure: |delta| x list dealer price (inventory value).
with adjustment as (
    select payload ->> 'movement_id' as movement_id, payload ->> 'warehouse_id' as warehouse_id,
           payload ->> 'sku' as sku, (payload ->> 'quantity_delta')::integer as quantity_delta,
           payload ->> 'source_ref' as source_ref, event_id, event_time
    from silver.events
    where event_type = 'mfg.inventory_movement' and payload ->> 'movement_type' = 'ADJUSTMENT'
),
approval as (
    select payload ->> 'approval_id' as approval_id, payload ->> 'warehouse_id' as warehouse_id,
           payload ->> 'sku' as sku, (payload ->> 'counted_quantity')::integer as counted_quantity,
           (payload ->> 'system_quantity')::integer as system_quantity,
           payload ->> 'approved_by_role' as approved_by_role, event_id, event_time
    from silver.events
    where event_type = 'mfg.stock_count_approval'
)
select
    a.movement_id                                           as subject_key,
    a.event_time                                            as anchor_time,
    (a.event_time at time zone 'Asia/Dhaka')::date          as business_date,
    'Warehouse ' || a.warehouse_id                          as business_unit,
    w.region_code                                           as region_code,
    abs(a.quantity_delta) * coalesce(p.dealer_price_bdt, 0) as exposure_bdt,
    'Adjustment of ' || a.quantity_delta || ' x ' || a.sku || ' at ' || a.warehouse_id || ' cites '
        || a.source_ref || case when ap.approval_id is null then ', which is not an approved stock count'
                                else ', an approval that does not match it' end as summary,
    jsonb_build_object(
        'transactions', jsonb_build_array(jsonb_build_object(
            'role', 'adjustment', 'event_type', 'mfg.inventory_movement', 'id', a.movement_id,
            'event_id', a.event_id, 'event_time', a.event_time, 'quantity', a.quantity_delta,
            'source_ref', a.source_ref))
            || case when ap.approval_id is null then '[]'::jsonb else jsonb_build_array(jsonb_build_object(
            'role', 'approval', 'event_type', 'mfg.stock_count_approval', 'id', ap.approval_id,
            'event_id', ap.event_id, 'event_time', ap.event_time,
            'quantity', ap.counted_quantity - ap.system_quantity)) end,
        'facts', jsonb_build_object('approval_found', ap.approval_id is not null,
                                    'unit_price_bdt', p.dealer_price_bdt)) as evidence
from adjustment a
left join approval ap on ap.approval_id = a.source_ref
left join ref.products p on p.sku = a.sku
left join ref.warehouses w on w.warehouse_id = a.warehouse_id
where ap.approval_id is null
   or ap.warehouse_id <> a.warehouse_id or ap.sku <> a.sku
   or ap.counted_quantity - ap.system_quantity <> a.quantity_delta
