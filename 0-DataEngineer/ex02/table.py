import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm

load_dotenv(dotenv_path="../.env")

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres'
DB_PORT = '5432'

# Función para crear la tabla desde el CSV
def create_table_from_csv(file_path):
    # Leer el archivo CSV usando pandas
    df = pd.read_csv(file_path)

    #print(df.head())
    # Obtener el nombre de la tabla a partir del nombre del archivo
    table_name = 'data_2022_dec'

    # Definir la creación de la tabla

    columns = [
        ("event_time", "TIMESTAMP"),
        ("event_type", "VARCHAR(255)"),
        ("product_id", "BIGINT"),
        ("price", "NUMERIC"),
        ("user_id", "INTEGER"),
        ("user_session", "TEXT")
    ]
    

    #print("COLS", columns)
    
    create_table_query = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, {})"
    ).format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(
            sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(col_type)) for col_name, col_type in columns
        )
    )
    

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    cursor = conn.cursor()

    cursor.execute(create_table_query)

    insert_query = sql.SQL(
        "INSERT INTO {} (event_time, event_type, product_id, price, user_id, user_session) VALUES (%s, %s, %s, %s, %s, %s)"
    ).format(sql.Identifier(table_name))

    for index, row in tqdm(df.iterrows()):
        cursor.execute(insert_query, (
            datetime.strptime(row['event_time'], '%Y-%m-%d %H:%M:%S UTC'),
            row['event_type'],
            row['product_id'],
            row['price'],
            row['user_id'],
            row['user_session']
        ))

    conn.commit()

    cursor.close()
    conn.close()
    print("\nConexión cerrada")



if __name__ == '__main__':
    csv_file_path = '../subject/customer/data_2022_dec.csv'

    try:
        create_table_from_csv(csv_file_path)
    except Exception as e:
        print(e)