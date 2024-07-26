import psycopg2
from dotenv import load_dotenv
import os


load_dotenv(dotenv_path="../.env")

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres'
DB_PORT = '5432'

sql_join = """
CREATE TEMPORARY TABLE temp_customers AS SELECT DISTINCT * FROM customers;
TRUNCATE customers;
INSERT INTO customers SELECT * FROM temp_customers;
"""


def remove_duplicates():
    """Join the tables"""
    
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    print("Connected to postgres!")
    cursor = conn.cursor()
    cursor.execute(sql_join)
    print("Duplicates have been removed.")
    conn.commit()

    cursor.close()
    conn.close()


if __name__ == "__main__":
    try:
        remove_duplicates()
    
    except Exception as error:
        print(f"An error occurred: {error}")
