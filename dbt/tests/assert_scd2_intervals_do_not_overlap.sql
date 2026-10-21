-- Each subscriber has exactly one valid version at any instant.
select a.subscriber_id, a.subscriber_sk, b.subscriber_sk as overlapping_sk
from {{ ref('dim_subscriber_scd2') }} a
join {{ ref('dim_subscriber_scd2') }} b
  on a.subscriber_id = b.subscriber_id and a.subscriber_sk <> b.subscriber_sk
 and a.valid_from < b.valid_to and b.valid_from < a.valid_to
