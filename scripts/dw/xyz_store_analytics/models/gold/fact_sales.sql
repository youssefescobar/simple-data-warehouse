WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

dim_products AS (
    SELECT product_id FROM {{ ref('dim_products') }}
),

dim_customers AS (
    SELECT customer_id FROM {{ ref('dim_customers') }}
),

dim_date AS (
    SELECT date_key, date FROM {{ ref('dim_date') }}
)

SELECT
    d.date_key,
    c.customer_id,
    p.product_id,
    o.order_id,
    oi.order_item_id,  -- Add grain key
    oi.quantity,
    oi.unit_price,
    (oi.quantity * oi.unit_price) AS total_amount,
    o.order_date,
    o.status AS order_status

FROM order_items AS oi

LEFT JOIN orders AS o
    ON oi.order_id = o.order_id

LEFT JOIN dim_date AS d 
    ON d.date = DATE(o.order_date)

LEFT JOIN dim_customers AS c 
    ON c.customer_id = o.customer_id

LEFT JOIN dim_products AS p 
    ON p.product_id = oi.product_id