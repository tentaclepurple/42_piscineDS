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
    DATE(event_time) AS date,
    COUNT(DISTINCT user_id) AS unique_users
FROM 
    customers_unique
WHERE 
    event_type = 'purchase'
    AND event_time BETWEEN '2022-10-01' AND '2023-01-31'
GROUP BY 
    DATE(event_time)
ORDER BY 
    date;
"""

total_rows_query = """
SELECT COUNT(*) AS total_days
FROM (
    SELECT 
        DATE(event_time) AS date
    FROM 
        customers_unique
    WHERE 
        event_type = 'purchase'
        AND event_time BETWEEN '2022-10-01' AND '2023-01-31'
    GROUP BY 
        DATE(event_time)
) AS daily_unique_users;
"""


def get_df():
    chunks = []
    
    total_rows = pd.read_sql(total_rows_query, engine).iloc[0, 0]
    
    for chunk in tqdm(pd.read_sql(query, engine, chunksize=10000), total=total_rows//10000, unit='chunk'):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    return df


def plot_daily_unique_customers(data):
    """Plot the number of unique customers per day"""

    # Convert 'date' to datetime
    data['date'] = pd.to_datetime(data['date'])
    
    # Set 'date' as index
    data.set_index('date', inplace=True)
    
    plt.figure(figsize=(12, 6))
    data['unique_users'].plot(kind='line', color='blue')
    plt.title('Unique Customers per Day (Purchases Only)')
    plt.xlabel('Date')
    plt.ylabel('Number of Customers')
    plt.grid(True, alpha=0.3)
    
    # Set x-axis ticks to show only the first day of each month
    months = pd.date_range(start=data.index.min(), end=data.index.max(), freq='MS')
    plt.xticks(months, [d.strftime('%b') for d in months], rotation=0)
    
    plt.tight_layout()
    plt.savefig('daily_customers_purchases.png')
    print("Chart saved as 'daily_customers_purchases.png'.")
    plt.close()


if __name__ == "__main__":
    start_time = time.time()

    try:
        #df = pd.read_sql(query, engine)
        df = get_df()
        print(df.head())
        plot_daily_unique_customers(df)

    except Exception as error:
        print(f"An error occurred: {error}")

    print("Execution time:", time.time() - start_time)