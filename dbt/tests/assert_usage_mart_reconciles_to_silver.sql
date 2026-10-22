select coalesce(s.usage_type, g.usage_type) as usage_type, s.qty as silver_qty, g.qty as gold_qty
from (select usage_type, sum(quantity) as qty from {{ ref('stg_tel_usage') }} group by 1) s
full outer join (select usage_type, sum(total_quantity) as qty from {{ ref('mart_telecom_usage_daily') }} group by 1) g
  on g.usage_type = s.usage_type
where s.qty is distinct from g.qty
