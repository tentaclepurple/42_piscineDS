import psycopg2
import os
from dotenv import load_dotenv
import time

load_dotenv(dotenv_path="../.env")

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST', 'postgres')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')

def export_table_to_csv(table_name: str):
	"""Export a table to a CSV file"""

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    print("Connected to postgres!")

    cursor = conn.cursor()
    output_query = f"COPY (SELECT * FROM {table_name}) TO STDOUT WITH CSV HEADER"
    with open(f'/app/{tablename + ".csv"}', 'w') as f:
        cursor.copy_expert(output_query, f)
        print(f"Table {table_name} has been exported to {table_name + ".csv"}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    start_time = time.time()
    try:
        export_table_to_csv("table_name")
    except Exception as error:
        print(f"An error occurred: {error}")
    
    print(f"--- {time.time() - start_time} seconds ---")
