select package_code, package_name, category, price_bdt, validity_days, data_mb, voice_minutes
from {{ source('ref', 'packages') }}
