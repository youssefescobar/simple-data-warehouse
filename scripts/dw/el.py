import psycopg2
import pandas as pd
from io import StringIO
from datetime import datetime

# --- Database Configurations ---

def get_db_connection(config):
    """Establishes a connection to the database."""
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        dbname=config['dbname']
    )

def extract_load(prod_conn, wh_conn, prod_table_name, wh_table_name, primary_key):
    """
    Extracts new/updated data from a prod table and "UPSERTS" it 
    into a bronze layer table.
    
    This function expects wh_conn to be managed (opened, closed, committed)
    by the caller.
    
    :param prod_conn: An active psycopg2 connection to the production DB.
    :param wh_conn: An active psycopg2 connection to the warehouse DB.
    :param prod_table_name: The source table name in 'public' schema (e.g., 'customers').
    :param wh_table_name: The destination table name in 'bronze' schema (e.g., 'raw_customers').
    :param primary_key: The primary key column of the table (e.g., 'customer_id').
    """
    print(f"\n--- Starting incremental load for: public.{prod_table_name} -> bronze.{wh_table_name} ---")
    
    wh_cursor = None
    try:
        wh_cursor = wh_conn.cursor()
        
        # 1. GET HIGH WATERMARK from Warehouse (Bronze)
        wh_cursor.execute(f"SELECT MAX(updated_at) FROM bronze.{wh_table_name};")
        high_watermark = wh_cursor.fetchone()[0]

        # 2. EXTRACT data from Production
        if high_watermark:
            print(f"- High watermark found: {high_watermark}")
            sql_query = f"SELECT * FROM public.{prod_table_name} WHERE updated_at > %s;"
            df = pd.read_sql(sql_query, prod_conn, params=(high_watermark,))
        else:
            print("- No high watermark. Performing first-time full load.")
            sql_query = f"SELECT * FROM public.{prod_table_name};"
            df = pd.read_sql(sql_query, prod_conn)

        if df.empty:
            print("- No new/updated data found. Load complete.")
            return

        print(f"- Extracted {len(df)} new/updated rows from production.")
        
        # 3. TRANSFORM (Add metadata columns)
        # (This assumes your bronze tables have these columns!)
        df['_extracted_at'] = datetime.now()
        df['_source_system'] = 'xyz_store_oltp'

        # 4. LOAD & UPSERT into Warehouse (Bronze)
        
        # A. Create a temporary staging table
        temp_table_name = f"staging_{wh_table_name}"
        wh_cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name};")
        wh_cursor.execute(f"CREATE UNLOGGED TABLE {temp_table_name} (LIKE bronze.{wh_table_name});")

        # B. Bulk-load DataFrame into the temp table
        output = StringIO()
        # This mapping is position-based. It's safe as long as `SELECT *` 
        # column order matches the `bronze` table order (plus new metadata cols)
        df.to_csv(output, sep='\t', header=False, index=False, na_rep='NULL')
        output.seek(0)
        
        wh_cursor.copy_expert(f"COPY {temp_table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL 'NULL')", output)
        print(f"- Loaded {len(df)} rows into temporary staging table.")

        # C. Perform the "UPSERT" using INSERT...ON CONFLICT
        # Get all columns from the destination bronze table
        wh_cursor.execute(f"""
            SELECT array_agg(column_name::text)
            FROM information_schema.columns
            WHERE table_schema = 'bronze' AND table_name = '{wh_table_name}';
        """)
        all_columns = wh_cursor.fetchone()[0]
        
        # Create the 'SET' clause for the update
        # e.g., "SET col1 = EXCLUDED.col1, col2 = EXCLUDED.col2, ..."
        update_set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in all_columns if col != primary_key])
        
        # Create the column list for insert
        column_list = ", ".join([f'"{col}"' for col in all_columns])

        upsert_sql = f"""
            INSERT INTO bronze.{wh_table_name} ({column_list})
            SELECT {column_list} FROM {temp_table_name}
            ON CONFLICT ({primary_key}) DO UPDATE
            SET {update_set_clause};
        """
        
        wh_cursor.execute(upsert_sql)
        print(f"- Upsert complete. Merged data into bronze.{wh_table_name}.")

        # D. Clean up
        wh_cursor.execute(f"DROP TABLE {temp_table_name};")

        # Commit changes *for this table*
        wh_conn.commit()
        print("- Successfully committed changes to warehouse.")

    except (Exception, psycopg2.Error) as error:
        print(f"\nError during incremental load for {prod_table_name}: {error}")
        if wh_conn:
            wh_conn.rollback() # Rollback this table's transaction
        raise # Re-raise error to stop the main() loop

    finally:
        if wh_cursor:
            wh_cursor.close()

def main():
    """
    Main function to orchestrate the EL pipeline.
    Manages connections and loops through tables.
    """
    
    prod_db_config = {
        "host": 'localhost',
        "port": 5432,
        "user": 'admin',
        "password": 'admin',
        "dbname" : 'xyz_store'
    }

    wh_db_config = {
        "host": 'localhost',
        "port": 5433,
        "user": 'admin',
        "password": 'admin',
        "dbname" : 'xyz_store_wh'
    }
    
    # Define all table mappings in one place
    table_mappings = [
        {"prod": "customers",   "wh": "raw_customers",   "pk": "customer_id"},
        {"prod": "products",    "wh": "raw_products",    "pk": "product_id"},
        {"prod": "orders",      "wh": "raw_orders",      "pk": "order_id"},
        {"prod": "order_items", "wh": "raw_order_items", "pk": "order_item_id"},
    ]
    
    prod_conn = None
    wh_conn = None
    
    try:
        # Create connections *once*
        prod_conn = get_db_connection(prod_db_config)
        wh_conn = get_db_connection(wh_db_config)
        
        print("--- Connections established ---")
        
        # Loop through table mappings
        for mapping in table_mappings:
            extract_load(
                prod_conn=prod_conn,
                wh_conn=wh_conn,
                prod_table_name=mapping["prod"],
                wh_table_name=mapping["wh"],
                primary_key=mapping["pk"]
            )
            
        print("\nAll tables loaded successfully!")
        
    except Exception as e:
        # The error is already printed by extract_load
        print(f"\nPipeline failed: {str(e)}")
        
    finally:
        # Close connections *once*
        if prod_conn:
            prod_conn.close()
        if wh_conn:
            wh_conn.close()
        print("\n--- Connections closed ---")
        
if __name__ == "__main__":
    main()

