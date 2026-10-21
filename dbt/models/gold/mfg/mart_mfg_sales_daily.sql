-- Grain: order business_date x dealer region x product category.
select
    business_date, dealer_region, category,
    count(*) as orders,
    sum(quantity) as ordered_qty,
    sum(order_value_bdt) as ordered_value_bdt,
    count(*) filter (where fulfilment_status in ('DISPATCHED', 'DELIVERED')) as shipped_orders,
    count(*) filter (where fulfilment_status = 'BACKORDERED') as backordered_orders,
    {{ kpi_status('mfg', 'o.business_date') }} as kpi_status
from {{ ref('fct_dealer_order') }} o
group by business_date, dealer_region, category
