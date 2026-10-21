select s.usage_type, s.qty as silver_qty, g.qty as gold_qty
from (select usage_type, sum(quantity) as qty from {{ ref('stg_tel_usage') }} group by 1) s
full outer join (select usage_type, sum(total_quantity) as qty from {{ ref('mart_telecom_usage_daily') }} group by 1) g
  using (usage_type)
where s.qty is distinct from g.qty
