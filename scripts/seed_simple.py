import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta
import sys


DB_NAME = "xyz_store"
DB_USER = "admin"
DB_PASS = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"
# ------------------------------------

# --- SEEDING CONFIGURATION ---
NUM_CUSTOMERS = 150
NUM_PRODUCTS = 75
NUM_ORDERS = 400
MAX_ITEMS_PER_ORDER = 5
# -----------------------------

# Initialize Faker
fake = Faker()

# Predefined lists for realistic data
PRODUCT_CATEGORIES = ['Electronics', 'Books', 'Clothing', 'Home & Kitchen', 'Groceries', 'Toys & Games', 'Sports & Outdoors']
ORDER_STATUSES = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']
# Weights for order statuses (e.g., most orders are delivered)
STATUS_WEIGHTS = [0.05, 0.1, 0.15, 0.6, 0.1] 

# Lists to store generated IDs for relationships
customer_data = []  # Stores (customer_id, created_at)
product_data = []   # Stores (product_id, price)

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error: Unable to connect to the database.")
        print(e)
        sys.exit(1)

def seed_customers(cursor):
    """Seeds the customers table with realistic data."""
    print(f"Seeding {NUM_CUSTOMERS} customers...")
    for _ in range(NUM_CUSTOMERS):
        full_name = fake.name()
        email = fake.unique.email()
        # Customers created in the last 2 years
        created_at = fake.date_time_between(start_date='-2y', end_date='now')
        
        cursor.execute(
            """
            INSERT INTO customers (full_name, email, created_at, updated_at)
            VALUES (%s, %s, %s, %s) RETURNING customer_id
            """,
            (full_name, email, created_at, created_at)
        )
        customer_id = cursor.fetchone()[0]
        customer_data.append((customer_id, created_at))
    print("Customers seeding complete.")

def seed_products(cursor):
    """Seeds the products table with realistic data."""
    print(f"Seeding {NUM_PRODUCTS} products...")
    for _ in range(NUM_PRODUCTS):
        category = random.choice(PRODUCT_CATEGORIES)
        name = f"{fake.color_name()} {fake.word().capitalize()} ({category})"
        
        # Fix: allow up to 1500 with 2 decimal places
        price = fake.pydecimal(left_digits=4, right_digits=2, positive=True, min_value=5, max_value=1500)
        
        created_at = fake.date_time_between(start_date='-3y', end_date='-1y')  # Products created before customers

        cursor.execute(
            """
            INSERT INTO products (name, category, price, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s) RETURNING product_id
            """,
            (name, category, price, created_at, created_at)
        )
        product_id = cursor.fetchone()[0]
        product_data.append((product_id, price))
    print("Products seeding complete.")


def seed_orders(cursor):
    """Seeds the orders table, ensuring orders are placed *after* a customer exists."""
    print(f"Seeding {NUM_ORDERS} orders...")
    order_data = [] # To store (order_id, order_date) for items
    
    for _ in range(NUM_ORDERS):
        # Pick a random customer and their sign-up date
        customer_id, customer_created_at = random.choice(customer_data)
        
        # Ensure order_date is *after* the customer was created
        order_date = fake.date_time_between_dates(
            datetime_start=customer_created_at,
            datetime_end=datetime.now()
        )
        
        status = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        
        # updated_at is between order_date and (at most) 10 days later
        # (or now if the order is very recent)
        max_update_date = min(order_date + timedelta(days=10), datetime.now())
        updated_at = fake.date_time_between_dates(
            datetime_start=order_date,
            datetime_end=max_update_date
        )

        cursor.execute(
            """
            INSERT INTO orders (customer_id, order_date, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s) RETURNING order_id
            """,
            (customer_id, order_date, status, order_date, updated_at) # Set created_at = order_date
        )
        order_id = cursor.fetchone()[0]
        order_data.append((order_id, order_date))
    
    print("Orders seeding complete.")
    return order_data

def seed_order_items(cursor, order_data):
    """Seeds order_items, snapshotting the price from the products table."""
    print("Seeding order items...")
    
    items_to_insert = []
    
    for order_id, order_date in order_data:
        num_items = random.randint(1, MAX_ITEMS_PER_ORDER)
        
        # Use a set to prevent adding the same product to the same order twice
        products_in_this_order = set()
        
        for _ in range(num_items):
            # Pick a random product and its price
            product_id, unit_price = random.choice(product_data)
            
            # If we've already added this product, skip to the next loop iteration
            if product_id in products_in_this_order:
                continue
                
            products_in_this_order.add(product_id)
            
            quantity = random.randint(1, 4)
            
            # Items created/updated at the same time as the order
            created_at = order_date
            updated_at = order_date
            
            items_to_insert.append(
                (order_id, product_id, quantity, unit_price, created_at, updated_at)
            )

    # Use execute_values for efficient bulk insertion
    from psycopg2.extras import execute_values
    execute_values(
        cursor,
        """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, created_at, updated_at)
        VALUES %s
        """,
        items_to_insert
    )
    print(f"Order items seeding complete ({len(items_to_insert)} items).")


def main():
    """Main function to run the seeder."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Clear tables in the correct order to avoid FK violations
        print("Clearing existing data...")
        cursor.execute("TRUNCATE order_items, orders, products, customers RESTART IDENTITY CASCADE")

        # Seed tables in the correct order
        seed_customers(cursor)
        seed_products(cursor)
        order_data = seed_orders(cursor)
        seed_order_items(cursor, order_data)
        
        # Commit the transaction
        conn.commit()
        print("\nDatabase seeding finished successfully!")

    except psycopg2.Error as e:
        print(f"\nAn error occurred during seeding:")
        print(e)
        if conn:
            conn.rollback() # Roll back changes on error
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()