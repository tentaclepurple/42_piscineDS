from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
from tqdm import tqdm

# Cargar las variables de entorno
load_dotenv(dotenv_path="../.env")

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres'
DB_PORT = '5432'

# Crear la URL de la base de datos
DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Crear el motor de SQLAlchemy
engine = create_engine(DATABASE_URL)

# Leer los datos en fragmentos y usar tqdm para la barra de progreso
def get_purchase_data():
    chunks = []
    query = "SELECT event_time, event_type, price FROM customers_unique WHERE event_type = 'purchase'"
    total_rows_query = "SELECT COUNT(*) FROM customers_unique WHERE event_type = 'purchase'"
    
    # Obtener el número total de filas
    total_rows = pd.read_sql(total_rows_query, engine).iloc[0, 0]
    
    # Leer los datos en fragmentos con tqdm para mostrar la barra de progreso
    for chunk in tqdm(pd.read_sql(query, engine, chunksize=10000), total=total_rows//10000, unit='chunk'):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    return df

def plot_number_of_purchases_per_month(data):
    data['event_time'] = pd.to_datetime(data['event_time'])
    monthly_purchases = data.set_index('event_time').resample('ME').size()
    
    plt.figure(figsize=(10, 6))
    monthly_purchases.plot(kind='bar', color='skyblue')
    plt.title('Number of Purchases per Month')
    plt.xlabel('Month')
    plt.ylabel('Number of Purchases')
    plt.savefig('number_of_purchases_per_month.png')
    plt.close()

def plot_total_purchase_price_per_month(data):
    data['event_time'] = pd.to_datetime(data['event_time'])
    monthly_revenue = data.set_index('event_time').resample('ME')['price'].sum()
    
    plt.figure(figsize=(10, 6))
    monthly_revenue.plot(kind='bar', color='green')
    plt.title('Total Purchase Price per Month')
    plt.xlabel('Month')
    plt.ylabel('Total Purchase Price (Altairian Dollars)')
    plt.savefig('total_purchase_price_per_month.png')
    plt.close()

def plot_number_of_purchases(data):
    event_counts = data['event_type'].value_counts()
    
    plt.figure(figsize=(10, 6))
    event_counts.plot(kind='pie', autopct='%1.1f%%', startangle=140, colors=['lightcoral'])
    plt.title('Distribution of Purchase Events')
    plt.ylabel('')
    plt.savefig('number_of_purchases.png')
    plt.close()

if __name__ == "__main__":
    try:
        # Recuperar los datos de compra
        purchase_data = get_purchase_data()

        # Crear los gráficos
        plot_number_of_purchases_per_month(purchase_data)
        plot_total_purchase_price_per_month(purchase_data)
        plot_number_of_purchases(purchase_data)
    except Exception as error:
        print(f"An error occurred: {error}")
