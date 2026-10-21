-- No Bronze row that was quarantined may also be the source of a Silver event.
select q.bronze_id, q.reason_code
from {{ source('silver', 'quarantine') }} q
join {{ source('silver', 'events') }} e on e.bronze_id = q.bronze_id
