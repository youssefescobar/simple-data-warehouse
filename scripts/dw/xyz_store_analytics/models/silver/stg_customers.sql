select customer_id,
lower(trim(email)) as email,
INITCAP(trim(full_name)) as full_name,
created_at,
updated_at

from {{source('bronze', 'raw_customers')}}
where customer_id is not null

