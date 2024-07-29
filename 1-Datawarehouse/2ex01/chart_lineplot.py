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
SELECT event_time, user_id FROM customers
WHERE event_type = 'purchase'
AND event_time BETWEEN '2022-10-01' AND '2023-02-28'
ORDER BY event_time;
"""

total_rows_query = """
    SELECT COUNT(*) FROM customers
    WHERE event_type = 'purchase'
    AND event_time BETWEEN '2022-10-01' AND '2023-02-28';
    """

def get_df():
    chunks = []
    
    total_rows = pd.read_sql(total_rows_query, engine).iloc[0, 0]
    
    for chunk in tqdm(pd.read_sql(query, engine, chunksize=10000), total=total_rows//10000, unit='chunk'):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    return df


def plot_daily_customers(data):
    """Plot the number of unique customers per day"""

    # Convert event_time pandas to datetime
    data['event_time'] = pd.to_datetime(data['event_time'])
    #ser event_time as datetime and set it as index
    data.set_index('event_time', inplace=True)
    
    # D is for daily
    #['user_id'] is the column to count
    #nunique() is the function to count unique values
    daily_customers = data.resample('D')['user_id'].nunique()
    
    plt.figure(figsize=(12, 6))
    daily_customers.plot(kind='line', color='blue')
    plt.title('Unique Customers per Day (Purchases Only)')
    plt.xlabel('Date')
    plt.ylabel('Number of Customers')
    plt.grid(True, alpha=0.3)
    
    # Set x-axis ticks to show only the first day of each month
    months = pd.date_range(start=daily_customers.index.min(), end=daily_customers.index.max(), freq='MS')
    plt.xticks(months, [d.strftime('%b') for d in months], rotation=0)
    
    plt.tight_layout()
    plt.savefig('lineplot_daily_customers.png')
    print("Chart saved as 'lineplot_daily_customers.png'.")
    plt.close()



if __name__ == "__main__":
    start_time = time.time()

    try:
        df = get_df()

        print(df.head())

        plot_daily_customers(df)

    except Exception as error:
        print(f"An error occurred: {error}")

    print(f"Execution time: {time.time() - start_time} seconds")
