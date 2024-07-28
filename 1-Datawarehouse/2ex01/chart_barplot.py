from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
from tqdm import tqdm
import time


load_dotenv(dotenv_path="../.env")

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres'
DB_PORT = '5432'

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(DATABASE_URL)

query = """
SELECT 
    DATE_TRUNC('month', event_time) AS month,
    SUM(price) /1000000 AS total_sales_millions
FROM 
    customers_unique
WHERE 
    event_type = 'purchase'
    AND event_time BETWEEN '2022-10-01' AND '2023-01-31'
GROUP BY 
    DATE_TRUNC('month', event_time)
ORDER BY 
    month;
"""

total_rows_query = """
SELECT COUNT(*)
FROM (
    SELECT 
        DATE_TRUNC('month', event_time) AS month,
        SUM(price) / 1000000 AS total_sales_millions
    FROM 
        customers_unique
    WHERE 
        event_type = 'purchase'
        AND event_time BETWEEN '2022-10-01' AND '2023-01-31'
    GROUP BY 
        DATE_TRUNC('month', event_time)
) AS subquery;

"""


def get_df():
    chunks = []
    
    total_rows = pd.read_sql(total_rows_query, engine).iloc[0, 0]
    
    #for chunk in pd.read_sql(query, engine, chunksize=10000):
    for chunk in tqdm(pd.read_sql(query, engine, chunksize=10000), total=total_rows//10000, unit='chunk'):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    return df


def plot_monthly_sales(data):
    """Plot the monthly sales"""

    # Convert event_time pandas to datetime
    data['date'] = pd.to_datetime(data['date'])
    
    # Set event_time as datetime and set it as index
    data.set_index('date', inplace=True)
    
    plt.figure(figsize=(12, 6))
    data['unique_users'].plot(kind='line', color='blue')
    plt.title('Unique Customers per Day (Purchases Only)')
    plt.xlabel('Date')
    plt.ylabel('Number of Customers')
    plt.grid(True, alpha=0.3)
    
    months = pd.date_range(start=data.index.min(), end=data.index.max(), freq='MS')
    plt.xticks(months, [d.strftime('%b') for d in months], rotation=0)
    
    plt.tight_layout()
    plt.savefig('daily_customers_purchases.png')
    print("Chart saved as 'daily_customers_purchases.png'.")
    plt.close()


if __name__ == "__main__":
    start_time = time.time()

    try:
        df = get_df()
        print(df)

        # Verificar duplicados en todas las columnas
        duplicates_all_columns = df.duplicated()
        print(f"Duplicados en todas las columnas: {duplicates_all_columns.sum()}")

        # Verificar duplicados basados en las columnas clave (event_time, product_id, price)
        duplicates_key_columns = df.duplicated(subset=['event_time', 'product_id', 'price'])
        print(f"Duplicados en las columnas clave: {duplicates_key_columns.sum()}")

        # Convertir event_time a datetime
        df['event_time'] = pd.to_datetime(df['event_time'])

        # Agrupar por mes y sumar los precios
        monthly_sales = df[df['event_type'] == 'purchase'].groupby(df['event_time'].dt.to_period('M'))['price'].sum()

        # Convertir a millones
        monthly_sales = monthly_sales / 1e6

        # Mostrar el resumen mensual
        print(monthly_sales)


        #plot_monthly_sales(df)

    except Exception as error:
        print(f"An error occurred: {error}")

    print("Execution time:", time.time() - start_time)