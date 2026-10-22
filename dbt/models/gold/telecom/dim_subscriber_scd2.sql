-- SCD type 2 built from full-state lifecycle snapshots, ordered by EVENT time (not arrival), so a late
-- lifecycle event lands in the right interval on the next rebuild.
with ordered as (
    select
        subscriber_id, segment, home_region, plan_type, action, event_id, event_time,
        case when action = 'TERMINATED' then 'TERMINATED' else 'ACTIVE' end as status,
        lead(event_time) over (partition by subscriber_id order by event_time, event_id) as next_event_time,
        row_number() over (partition by subscriber_id order by event_time desc, event_id desc) as rn_desc
    from {{ ref('stg_tel_subscriber_lifecycle') }}
)
select
    {{ md5_hex("subscriber_id || '|' || cast(event_id as varchar)") }} as subscriber_sk,
    subscriber_id, segment, home_region, plan_type, status,
    action as change_reason,
    event_time as valid_from,
    coalesce(next_event_time, {{ open_end_ts() }}) as valid_to,
    rn_desc = 1 as is_current,
    event_id as source_event_id
from ordered
