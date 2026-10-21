-- Total on-hand per warehouse must equal the sum of accepted inventory movements in Silver.
with silver as (
    select warehouse_id, sum(quantity_delta) as qty from {{ ref('stg_mfg_inventory_movement') }} group by 1
),
gold as (
    select warehouse_id, sum(on_hand_qty) as qty from {{ ref('mart_inventory_position') }} group by 1
)
select coalesce(s.warehouse_id, g.warehouse_id) as warehouse_id, s.qty as silver_qty, g.qty as gold_qty
from silver s full outer join gold g on g.warehouse_id = s.warehouse_id
where s.qty is distinct from g.qty
