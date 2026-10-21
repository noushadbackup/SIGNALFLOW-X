-- Grain: business_date x factory x category, including days a factory reported a disruption and produced nothing.
with produced as (
    select b.business_date, b.factory_id, p.category, count(*) as batches, sum(b.quantity) as units
    from {{ ref('stg_mfg_production_batch') }} b
    join {{ ref('dim_product') }} p on p.sku = b.sku
    group by 1, 2, 3
),
disrupted as (
    select business_date, factory_id, max(reason) as disruption_reason, max(severity) as disruption_severity
    from {{ ref('stg_mfg_supply_disruption') }}
    group by 1, 2
)
select
    coalesce(p.business_date, d.business_date) as business_date,
    coalesce(p.factory_id, d.factory_id) as factory_id,
    p.category,
    coalesce(p.batches, 0) as batches,
    coalesce(p.units, 0) as units,
    d.disruption_reason,
    d.disruption_severity
from produced p
full outer join disrupted d on d.business_date = p.business_date and d.factory_id = p.factory_id
