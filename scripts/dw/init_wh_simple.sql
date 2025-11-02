CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE bronze.raw_customers (
    customer_id INTEGER,
    email VARCHAR(255),
    full_name VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_system VARCHAR(50) DEFAULT 'xyz_store_oltp'
);

CREATE TABLE bronze.raw_products (
    product_id INTEGER,
    name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_system VARCHAR(50) DEFAULT 'xyz_store_oltp'
);

CREATE TABLE bronze.raw_orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date TIMESTAMP,
    status VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_system VARCHAR(50) DEFAULT 'xyz_store_oltp'
);

CREATE TABLE bronze.raw_order_items (
    order_item_id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_system VARCHAR(50) DEFAULT 'xyz_store_oltp'
);