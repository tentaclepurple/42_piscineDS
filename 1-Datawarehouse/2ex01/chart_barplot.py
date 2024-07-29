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
    customers
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
    customers
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
    
    for chunk in tqdm(pd.read_sql(query, engine, chunksize=10000), total=total_rows//10000, unit='chunk'):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    return df


def plot_monthly_sales(data):
    """Bar plot the total sales"""

    data["total_sales_millions"] = data["total_sales_millions"]*0.8
    
    # Convert event_time pandas to datetime
    data['month'] = pd.to_datetime(df['month'])
    
    data['month'] = data['month'].dt.strftime('%b')
    
    # Set event_time as datetime and set it as index
#    data.set_index('date', inplace=True)
    
    

    # Crear el bar plot
    plt.figure(figsize=(10, 6))
    plt.bar(data['month'], data['total_sales_millions'], color='blue')

    # Añadir títulos y etiquetas
    plt.title('Total Sales in Millions of Altarians')
    plt.xlabel('Month')
    plt.ylabel('Total Sales (Millions)')
    plt.ylim(0, max(data['total_sales_millions']) * 1.2)  # Asegurar un poco de espacio por encima de la barra más alta
    plt.grid(axis='y', alpha=0.3)
    plt.savefig('barplot_monthly_sales.png')
    plt.close()


if __name__ == "__main__":
    start_time = time.time()

    try:
        df = get_df()

        plot_monthly_sales(df)

    except Exception as error:
        print(f"An error occurred: {error}")

    print("Execution time:", time.time() - start_time)