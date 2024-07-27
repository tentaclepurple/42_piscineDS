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

sql_remove_dups = """
CREATE TABLE customers_unique AS
SELECT DISTINCT * FROM customers;
"""

def remove_dups():
    """Remove duplicates from the fusion table"""
    
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    print("Connected to postgres!")
    cursor = conn.cursor()
    cursor.execute(sql_remove_dups)
    print("Removed duplicates from 'fusion'.")

    conn.commit()

    cursor.close()
    conn.close()

if __name__ == "__main__":

    start_time = time.time()
    try:
        remove_dups()
    except Exception as error:
        print(f"An error occurred: {error}")

    print(f"--- {time.time() - start_time} seconds ---")
