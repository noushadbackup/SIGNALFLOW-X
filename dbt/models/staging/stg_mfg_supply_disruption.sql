select {{ stg_common() }},
    {{ p('disruption_id') }} as disruption_id,
    {{ p('factory_id') }} as factory_id,
    {{ p('reason') }} as reason,
    {{ p('expected_duration_hours', 'integer') }} as expected_duration_hours,
    {{ p('severity') }} as severity
from {{ source('silver', 'events') }}
where event_type = 'mfg.supply_disruption'
