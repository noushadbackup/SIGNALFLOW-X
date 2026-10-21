-- Calendar in the Asia/Dhaka reporting timezone. Bangladesh weekend: Friday and Saturday.
with bounds as (
    select min({{ dhaka_date('event_time') }}) as first_day,
           max({{ dhaka_date('event_time') }}) + 90 as last_day   -- headroom for invoice due dates
    from {{ source('silver', 'events') }}
)
select
    d::date as date_day,
    extract(isodow from d)::int as iso_day_of_week,
    to_char(d, 'Dy') as day_name,
    extract(isodow from d) in (5, 6) as is_weekend,
    date_trunc('month', d)::date as month_start,
    extract(year from d)::int as year
from bounds, generate_series(first_day, last_day, interval '1 day') as d
