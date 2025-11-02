import random
from faker import Faker
import psycopg2
import psycopg2.extras # For high-performance batch inserts
from tqdm import tqdm
from datetime import datetime

fake = Faker()
Faker.seed(42)
random.seed(42)

# ------------------------
# Database connection
# ------------------------
try:
    conn = psycopg2.connect(
        host="localhost",  # or the Docker service name (e.g. 'db')
        port=5432,
        user="admin",
        password="admin",
        dbname="xyz_store"
    )
    cur = conn.cursor()

    # ------------------------
    # Helpers
    # ------------------------
    def execute_batch(query, data):
        """Uses execute_values for efficient batch insertion."""
        if not data:
            return
        psycopg2.extras.execute_values(cur, query, data)
        conn.commit()

    def execute_batch_and_fetch(query, data):
        """Uses execute_values and fetches RETURNING results."""
        if not data:
            return []
        # The 'fetch=True' argument is key here
        return psycopg2.extras.execute_values(cur, query, data, fetch=True)

    # ------------------------
    # USERS
    # ------------------------
    roles = ["customer", "seller", "admin"]
    def clean_phone():
        return fake.msisdn()[:20]

    print("Seeding users...")
    users = [
        (
            fake.name(),
            fake.unique.email(),
            fake.password(length=12),
            clean_phone(),
            random.choices(roles, weights=[0.8, 0.18, 0.02])[0]
        )
        for _ in range(500)
    ]

    # Insert users and fetch their actual IDs
    user_results = execute_batch_and_fetch("""
        INSERT INTO users (full_name, email, password_hash, phone, role)
        VALUES %s
        RETURNING id, role
    """, users)
    
    # Create lists of actual IDs
    user_ids = [r[0] for r in user_results]
    customer_ids = [r[0] for r in user_results if r[1] == 'customer']
    seller_ids = [r[0] for r in user_results if r[1] == 'seller']

    print(f"✅ Inserted {len(user_ids)} users")


    # ------------------------
    # ADDRESSES
    # ------------------------
    print("Seeding addresses...")
    addresses = []
    # Use the actual user_ids list
    for user_id in user_ids:
        for _ in range(random.randint(1, 2)):
            addresses.append((
                user_id,
                fake.street_address(),
                fake.secondary_address(),
                fake.city(),
                fake.state(),
                fake.zipcode(),
                fake.country(),
                random.choice([True, False])
            ))

    execute_batch("""
        INSERT INTO addresses (user_id, address_line1, address_line2, city, state, postal_code, country, is_default)
        VALUES %s
    """, addresses)
    
    # Fetch all address IDs for later use in orders
    cur.execute("SELECT id FROM addresses")
    all_address_ids = [r[0] for r in cur.fetchall()]

    print(f"✅ Inserted {len(addresses)} addresses")


    # ------------------------
    # CATEGORIES (hierarchical)
    # ------------------------
    print("Seeding categories...")
    main_categories = ["Electronics", "Fashion", "Home", "Sports", "Books"]
    category_data = [(name, None) for name in main_categories]

    # Insert main categories and fetch their IDs
    main_cat_ids = execute_batch_and_fetch(
        "INSERT INTO categories (name, parent_id) VALUES %s RETURNING id", 
        category_data
    )

    # Subcategories
    subcategories = []
    for (cat_id,) in main_cat_ids: # Unpack the tuple
        for _ in range(random.randint(2, 5)):
            subcategories.append((f"{fake.word().capitalize()}", cat_id))

    # Insert subcategories and fetch their IDs
    subcategory_ids = execute_batch_and_fetch(
        "INSERT INTO categories (name, parent_id) VALUES %s RETURNING id", 
        subcategories
    )
    subcategory_ids = [r[0] for r in subcategory_ids] # Flatten list

    print("✅ Inserted categories and subcategories")


    # ------------------------
    # PRODUCTS
    # ------------------------
    print("Seeding products...")
    products = []
    for _ in tqdm(range(3000), desc="Generating products"):
        products.append((
            f"{fake.word().capitalize()} {random.choice(['Phone', 'Laptop', 'Watch', 'Jacket', 'Chair', 'Book'])}", # name
            fake.text(120), # description
            round(random.uniform(10, 1500), 2), # price
            random.choice(subcategory_ids), # category_id
            random.choice(seller_ids) # seller_id
        ))

    # Insert products and fetch their ID and Price
    product_results = execute_batch_and_fetch("""
        INSERT INTO products (name, description, price, category_id, seller_id)
        VALUES %s
        RETURNING id, price
    """, products)

    # Create a list of product IDs and a price lookup map
    product_ids = [r[0] for r in product_results]
    product_price_map = {r[0]: r[1] for r in product_results} # {product_id: price}

    print("✅ Inserted products")


    # ------------------------
    # INVENTORY
    # ------------------------
    print("Seeding inventory...")
    inventory = [
        (pid, random.randint(0, 200))
        for pid in product_ids
    ]
    execute_batch("""
        INSERT INTO inventory (product_id, stock_quantity)
        VALUES %s
    """, inventory)
    print("✅ Inserted inventory")


    # ------------------------
    # PRODUCT IMAGES
    # ------------------------
    print("Seeding product images...")
    images = []
    for pid in product_ids:
        for _ in range(random.randint(1, 3)):
            images.append((
                pid,
                fake.image_url(),
                random.choice([True, False])
            ))
    execute_batch("""
        INSERT INTO product_images (product_id, image_url, is_primary)
        VALUES %s
    """, images)
    print("✅ Inserted product images")


    # ------------------------
    # ORDERS, ORDER ITEMS, PAYMENTS
    # ------------------------
    print("Seeding orders, items, and payments...")
    orders_to_insert = []
    # We'll map a temporary local index to the items and payment for that order
    order_item_map = {} 
    payments_map = {}
    
    statuses = ["pending", "paid", "shipped", "delivered", "cancelled"]

    for customer_id in tqdm(customer_ids, desc="Generating orders"):
        for _ in range(random.randint(0, 5)): # 0 to 5 orders per customer
            total = 0
            num_items = random.randint(1, 4)
            selected_products = random.sample(product_ids, num_items)
            
            items_for_this_order = []
            for pid in selected_products:
                qty = random.randint(1, 3)
                price = product_price_map[pid] # Get the *actual* product price
                total += price * qty
                items_for_this_order.append((pid, qty, price))
            
            status = random.choice(statuses)
            local_index = len(orders_to_insert)

            # 1. Stage the Order data
            orders_to_insert.append((
                customer_id, 
                random.choice(all_address_ids), # Use a random real address
                total, 
                status,
                fake.date_time_between(start_date="-2y") # Add a realistic timestamp
            ))
            
            # 2. Stage the Order Items (using local index as key)
            order_item_map[local_index] = items_for_this_order
            
            # 3. Stage the Payment (using local index as key)
            payments_map[local_index] = (
                random.choice(["credit_card", "paypal", "stripe"]), 
                total, 
                "paid" if status not in ["pending", "cancelled"] else status, 
                fake.uuid4() if status != "pending" else None
            )

    # Now, insert all orders and get their real IDs
    inserted_order_ids = execute_batch_and_fetch("""
        INSERT INTO orders (user_id, address_id, total_amount, status, created_at)
        VALUES %s
        RETURNING id
    """, orders_to_insert)

    # Prepare final lists for order_items and payments
    order_items_to_insert = []
    payments_to_insert = []

    for i, (order_id,) in enumerate(inserted_order_ids):
        # Link items to the real order_id
        for (pid, qty, price) in order_item_map[i]:
            order_items_to_insert.append((order_id, pid, qty, price))
        
        # Link payment to the real order_id
        payment_data = payments_map[i]
        payments_to_insert.append((order_id, *payment_data))

    # Batch insert the dependent data
    execute_batch("""
        INSERT INTO order_items (order_id, product_id, quantity, price)
        VALUES %s
    """, order_items_to_insert)
    
    execute_batch("""
        INSERT INTO payments (order_id, payment_method, amount, status, transaction_id)
        VALUES %s
    """, payments_to_insert)

    print("✅ Inserted orders, items, payments")


    # ------------------------
    # REVIEWS
    # ------------------------
    print("Seeding reviews...")
    # Use a set to guarantee (user_id, product_id) uniqueness
    review_pairs = set()
    max_reviews = 4000
    
    # We already have `user_ids`, so we can use that directly
    
    with tqdm(total=max_reviews, desc="Generating unique reviews") as pbar:
        while len(review_pairs) < max_reviews:
            pair = (random.choice(user_ids), random.choice(product_ids))
            if pair not in review_pairs:
                review_pairs.add(pair)
                pbar.update(1)

    reviews = [
        (
            uid,
            pid,
            random.randint(1, 5),
            fake.sentence(),
            fake.date_time_between(start_date="-2y") # Use realistic timestamps
        )
        for (uid, pid) in review_pairs
    ]

    execute_batch("""
        INSERT INTO reviews (user_id, product_id, rating, comment, created_at)
        VALUES %s
    """, reviews)
    print(f"✅ Inserted {len(reviews)} reviews")

    
    # ------------------------
    # CART ITEMS
    # ------------------------
    print("Seeding cart items...")
    # Use a set to guarantee (user_id, product_id) uniqueness
    cart_pairs = set()
    
    # Let's add cart items for about 20% of customers
    num_carts = len(customer_ids) // 5 
    
    with tqdm(total=num_carts, desc="Generating carts") as pbar:
        while len(cart_pairs) < num_carts:
            # Pick a random customer and a random product
            pair = (random.choice(customer_ids), random.choice(product_ids))
            if pair not in cart_pairs:
                cart_pairs.add(pair)
                pbar.update(1)

    cart_items = [
        (
            uid,
            pid,
            random.randint(1, 3) # quantity
        )
        for (uid, pid) in cart_pairs
    ]

    execute_batch("""
        INSERT INTO cart_items (user_id, product_id, quantity)
        VALUES %s
    """, cart_items)
    print(f"✅ Inserted {len(cart_items)} cart items")


    # ------------------------
    # SHIPMENTS
    # ------------------------
    print("Seeding shipments...")
    
    # First, get the IDs of orders that are 'shipped' or 'delivered'
    cur.execute("SELECT id, status, created_at FROM orders WHERE status IN ('shipped', 'delivered')")
    shippable_orders = cur.fetchall()
    
    shipments = []
    carriers = ["DHL", "FedEx", "UPS", "USPS"]
    
    for order_id, status, created_at in tqdm(shippable_orders, desc="Generating shipments"):
        # Ensure shipped_at is after created_at
        shipped_at = fake.date_time_between(start_date=created_at)
        delivered_at = None
        if status == 'delivered':
            # Ensure delivered_at is after shipped_at
            delivered_at = fake.date_time_between(start_date=shipped_at)
            
        shipments.append((
            order_id,
            random.choice(carriers),
            fake.bothify(text='1Z##################').upper(), # Tracking number
            shipped_at,
            delivered_at
        ))

    execute_batch("""
        INSERT INTO shipments (order_id, carrier, tracking_number, shipped_at, delivered_at)
        VALUES %s
    """, shipments)
    print(f"✅ Inserted {len(shipments)} shipments")


except (Exception, psycopg2.DatabaseError) as error:
    print(f"❌ Error seeding database: {error}")
    conn.rollback() # Roll back any changes on error

finally:
    # ------------------------
    # Done
    # ------------------------
    if 'conn' in locals() and conn:
        cur.close()
        conn.close()
        print("\n🎉 Database seeded successfully and connection closed.")