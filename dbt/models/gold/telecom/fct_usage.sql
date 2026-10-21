-- Grain: one rated usage record (usage_id).
select usage_id, event_id, event_time, business_date, subscriber_id, usage_type, quantity, unit, serving_region,
       cell_area_code, rated_amount_bdt, arrival_class
from {{ ref('stg_tel_usage') }}
