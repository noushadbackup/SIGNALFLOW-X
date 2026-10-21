select warehouse_id, warehouse_name, region_code, is_central from {{ source('ref', 'warehouses') }}
