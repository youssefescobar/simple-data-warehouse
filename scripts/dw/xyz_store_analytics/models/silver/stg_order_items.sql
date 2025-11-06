select 
order_item_id,
order_id,
product_id,
quantity,
unit_price,
created_at,
updated_at

from {{source('bronze', 'raw_order_items')}}
where order_item_id is not null
