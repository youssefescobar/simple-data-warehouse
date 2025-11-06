select 
product_id,
lower(trim(name)) as name,
lower(trim(category)) as category,
price,
created_at,
updated_at

from {{source('bronze', 'raw_products')}}

where product_id is not null