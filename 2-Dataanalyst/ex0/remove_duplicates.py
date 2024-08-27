import psycopg2
from dotenv import load_dotenv
import os
import time

start_time = time.time()

load_dotenv(dotenv_path="../.env")

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres'
DB_PORT = '5432'


#cleans duplicates from the item table
sql_clean_item = """
DROP TABLE IF EXISTS item_clean;
CREATE TABLE item_clean AS
SELECT
    product_id,
    COALESCE(MAX(category_id), NULL) AS category_id,
    COALESCE(MAX(category_code), NULL) AS category_code,
    COALESCE(MAX(brand), NULL) AS brand
FROM
    item
GROUP BY
    product_id;
"""

#cleans duplicates from the customers table
sql_clean_customers = """
DROP TABLE IF EXISTS customers_clean;
CREATE TABLE customers_clean AS
WITH ranked_events AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY event_type, product_id, price, user_id, user_session
           ORDER BY event_time
         ) AS rn
  FROM customers
)
SELECT 
  event_time, event_type, product_id, price, user_id, user_session
FROM ranked_events
WHERE rn = 1
ORDER BY event_time;
"""


def remove_duplicates():
    """Remove duplicates from the item and customers tables"""

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    print("Connected to postgres!")
    cursor = conn.cursor()
    cursor.execute(sql_clean_item)
    actual_time = time.time() - start_time
    print(f"Items table cleaned. {actual_time} seconds")
    cursor.execute(sql_clean_customers)
    actual_time = time.time() - actual_time
    print(f"Customers table cleaned. {actual_time} seconds")
    conn.commit()

    cursor.close()
    conn.close()


if __name__ == "__main__":
    try:
        remove_duplicates()
    
    except Exception as error:
        print(f"An error occurred: {error}")
