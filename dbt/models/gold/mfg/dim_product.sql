select sku, product_name, category, dealer_price_bdt from {{ source('ref', 'products') }}
