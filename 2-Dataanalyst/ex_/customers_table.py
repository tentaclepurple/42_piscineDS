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

sql_join = """
CREATE TABLE IF NOT EXISTS customers AS (
    SELECT * FROM data_2022_dec
    UNION ALL
    SELECT * FROM data_2022_nov
    UNION ALL
    SELECT * FROM data_2022_oct
    UNION ALL
    SELECT * FROM data_2023_feb
    UNION ALL
    SELECT * FROM data_2023_jan
);
"""


def join_tables():
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
    print("Data has been fetched from the table.")
    conn.commit()

    cursor.close()
    conn.close()


if __name__ == "__main__":

    start_time = time.time()

    try:
        join_tables()
    
    except Exception as error:
        print(f"An error occurred: {error}")
    
    print(f"--- {time.time() - start_time} seconds ---")
