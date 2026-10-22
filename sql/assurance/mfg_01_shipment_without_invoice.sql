-- MFG-01  Product shipment has no corresponding invoice.
-- Subject: shipment_id. Anchor: first shipment event time (normally DISPATCHED). Exposure: quantity times the
-- order's unit price, or the list dealer price when the order is not in Silver.
with shipment as (
    select payload ->> 'shipment_id' as shipment_id, min(event_time) as shipped_at,
           min(payload ->> 'order_id') as order_id, min(payload ->> 'warehouse_id') as warehouse_id,
           min(payload ->> 'sku') as sku, max((payload ->> 'quantity')::integer) as quantity,
           jsonb_agg(jsonb_build_object(
               'role', 'shipment', 'event_type', 'mfg.shipment', 'id', payload ->> 'shipment_id',
               'event_id', event_id, 'event_time', event_time, 'status', payload ->> 'status',
               'quantity', (payload ->> 'quantity')::integer) order by event_time) as transactions
    from silver.events
    where event_type = 'mfg.shipment'
    group by 1
),
invoiced as (
    select distinct payload ->> 'shipment_id' as shipment_id from silver.events where event_type = 'mfg.invoice'
),
orders as (
    select payload ->> 'order_id' as order_id, payload ->> 'dealer_id' as dealer_id,
           (payload ->> 'unit_price_bdt')::numeric as unit_price_bdt, event_id, event_time
    from silver.events
    where event_type = 'mfg.dealer_order'
)
select
    s.shipment_id                                           as subject_key,
    s.shipped_at                                            as anchor_time,
    (s.shipped_at at time zone 'Asia/Dhaka')::date          as business_date,
    'Warehouse ' || s.warehouse_id                          as business_unit,
    w.region_code                                           as region_code,
    s.quantity * coalesce(o.unit_price_bdt, p.dealer_price_bdt) as exposure_bdt,
    s.quantity || ' x ' || s.sku || ' shipped from ' || s.warehouse_id || ' (order ' || s.order_id
        || ') without an invoice'                           as summary,
    jsonb_build_object(
        'transactions', s.transactions || case when o.order_id is null then '[]'::jsonb else jsonb_build_array(
            jsonb_build_object('role', 'order', 'event_type', 'mfg.dealer_order', 'id', o.order_id,
                               'event_id', o.event_id, 'event_time', o.event_time, 'dealer_id', o.dealer_id,
                               'unit_price_bdt', o.unit_price_bdt)) end,
        'facts', jsonb_build_object('invoices_found', 0, 'priced_from',
                                    case when o.order_id is null then 'list price' else 'order' end)) as evidence
from shipment s
left join orders o on o.order_id = s.order_id
left join ref.products p on p.sku = s.sku
left join ref.warehouses w on w.warehouse_id = s.warehouse_id
where not exists (select 1 from invoiced i where i.shipment_id = s.shipment_id)
