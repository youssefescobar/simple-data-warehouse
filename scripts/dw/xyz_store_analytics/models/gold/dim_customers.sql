SELECT 
    customer_id, 
    full_name, 
    email, 
    created_at, 
    updated_at
FROM {{ ref('stg_customers') }}