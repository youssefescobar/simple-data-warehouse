select
order_id,
customer_id,
order_date,
trim(status) as status,
created_at,
updated_at 

from {{source('bronze', 'raw_orders')}}

where order_id is not null