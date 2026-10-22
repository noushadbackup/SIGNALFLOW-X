-- Calendar in the Asia/Dhaka reporting timezone. Bangladesh weekend: Friday and Saturday.
with bounds as (
    select min({{ dhaka_date('event_time') }}) as first_day,
           {{ add_days('max(' ~ dhaka_date('event_time') ~ ')', 90) }} as last_day   -- headroom for invoice due dates
    from {{ source('silver', 'events') }}
),
days as (
    select cast(d as date) as date_day
    from bounds cross join {{ day_series('first_day', 'last_day') }}
)
select
    date_day,
    {{ iso_dow('date_day') }} as iso_day_of_week,
    {{ day_name('date_day') }} as day_name,
    {{ iso_dow('date_day') }} in (5, 6) as is_weekend,
    cast(date_trunc('month', date_day) as date) as month_start,
    cast(extract(year from date_day) as integer) as year
from days
