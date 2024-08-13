from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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
    day,
    AVG(user_total_spend) AS average_spend_per_user
FROM (
    SELECT 
        DATE(event_time) AS day,
        user_id,
        SUM(price) AS user_total_spend
    FROM 
        customers
    WHERE
        event_type = 'purchase'
        AND event_time BETWEEN '2022-10-01' AND '2023-01-31'
    GROUP BY 
        DATE(event_time), user_id
) AS user_daily_spend
GROUP BY 
    day
ORDER BY 
    day;
"""

total_rows_query = """
SELECT 
    COUNT(*) AS num_rows
FROM (
    SELECT 
        day,
        AVG(user_total_spend) AS average_spend_per_user
    FROM (
        SELECT 
            DATE(event_time) AS day,
            user_id,
            SUM(price) AS user_total_spend
        FROM 
            customers
        WHERE
            event_type = 'purchase'
            AND event_time BETWEEN '2022-10-01' AND '2023-01-31'
        GROUP BY 
            DATE(event_time), user_id
    ) AS user_daily_spend
    GROUP BY 
        day
    ORDER BY 
        day
) AS result;
"""


def get_df():
    chunks = []
    
    total_rows = pd.read_sql(total_rows_query, engine).iloc[0, 0]
    
    for chunk in tqdm(pd.read_sql(query, engine, chunksize=10000), total=total_rows//10000, unit='chunk'):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    return df


def plot_average_sales(data):
    """Bar plot the total sales"""

    data["average_spend_per_user"] = data["average_spend_per_user"] * 0.8
    
    data['day'] = pd.to_datetime(data['day'])

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.fill_between(df['day'], df['average_spend_per_user'], alpha=0.5)

    ax.set_ylabel('verage spend/customer in Altairians')

    #Adjust the x-axis according to your data
    ax.set_xlim(pd.Timestamp('2022-10-01'), pd.Timestamp('2023-01-31'))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))

    max_spend = df['average_spend_per_user'].max()
    ax.set_ylim(0, max_spend * 1.1)

    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    plt.savefig('fillbetween_average_spend.png')
    plt.close()


if __name__ == "__main__":
    start_time = time.time()

    try:
        df = get_df()
        print(df)

        plot_average_sales(df)

    except Exception as error:
        print(f"An error occurred: {error}")

    print("Execution time:", time.time() - start_time)