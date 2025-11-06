SELECT 
    product_id,
    name, 
    category, 
    price, 
    created_at, 
    updated_at
FROM {{ ref('stg_products') }}