-- Grain: one dealer order line (order_id). Status is derived from later status and shipment events.
with status_events as (
    select order_id, status as last_status
    from (
        select order_id, status,
               row_number() over (partition by order_id order by event_time desc, event_id desc) as rn
        from {{ ref('stg_mfg_order_status') }}
    ) s
    where rn = 1
)
select
    o.order_id, o.event_id, o.event_time as ordered_at, o.business_date, o.dealer_id, d.region_code as dealer_region,
    o.sku, p.category, o.quantity, o.unit_price_bdt, o.quantity * o.unit_price_bdt as order_value_bdt,
    sh.shipment_id, inv.invoice_id,
    case
        when sh.current_status = 'DELIVERED' then 'DELIVERED'
        when sh.shipment_id is not null then 'DISPATCHED'
        when st.last_status is not null then st.last_status
        else 'OPEN'
    end as fulfilment_status,
    o.arrival_class
from {{ ref('stg_mfg_dealer_order') }} o
left join {{ ref('dim_dealer') }} d on d.dealer_id = o.dealer_id
left join {{ ref('dim_product') }} p on p.sku = o.sku
left join {{ ref('fct_shipment') }} sh on sh.order_id = o.order_id
left join {{ ref('stg_mfg_invoice') }} inv on inv.shipment_id = sh.shipment_id
left join status_events st on st.order_id = o.order_id
