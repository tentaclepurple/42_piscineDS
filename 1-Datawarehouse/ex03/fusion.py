import psycopg2
from dotenv import load_dotenv
import os
import time

load_dotenv(dotenv_path="../.env")

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres'
DB_PORT = '5432'

sql_fusion = """
DROP TABLE IF EXISTS new_customers;
CREATE TABLE new_customers AS
SELECT
    c.event_time,
    c.event_type,
    c.product_id,
    c.price,
    c.user_id,
    c.user_session,
    i.category_id,
    i.category_code,
    i.brand
FROM
    customers_clean c
JOIN
    item_clean i
ON
    c.product_id = i.product_id;
"""

sql_rename = """
ALTER TABLE customers RENAME TO old_customers;
ALTER TABLE new_customers RENAME TO customers;
"""

def backup_and_fusion_tables():
    """Backup the customers table and fuse it with the item table"""

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    print("Connected to postgres!")
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql_fusion)
        print("Tables 'customers' and 'item' have been fused into 'new_customers'.")
        
        cursor.execute(sql_rename)
        print("Renamed 'new_customers' to 'customers' and 'customers' to 'old_customers'.")
        
        conn.commit()

    except Exception as error:
        print(f"An error occurred: {error}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    start_time = time.time()

    try:
        backup_and_fusion_tables()
    except Exception as error:
        print(f"An error occurred: {error}")

    print(f"--- {time.time() - start_time} seconds ---")
