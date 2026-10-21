select region_code, region_name, demand_weight from {{ source('ref', 'regions') }}
