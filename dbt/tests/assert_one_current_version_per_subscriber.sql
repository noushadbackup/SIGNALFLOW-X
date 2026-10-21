select subscriber_id, count(*) filter (where is_current) as current_versions
from {{ ref('dim_subscriber_scd2') }}
group by subscriber_id
having count(*) filter (where is_current) <> 1
