from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import numpy as np
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
WITH session_totals AS (
    SELECT
        user_id,
        user_session,
        SUM(price) AS session_cart_total
    FROM
        customers
    WHERE
        event_type = 'purchase'
    GROUP BY
        user_id, user_session
)
SELECT
    user_id,
    AVG(session_cart_total) AS avg_cart_price
FROM
    session_totals
GROUP BY
    user_id
HAVING
	AVG(session_cart_total) BETWEEN 26 AND 43;
"""

total_rows_query = """
SELECT COUNT(*) FROM (
    SELECT
        user_id, AVG(price) AS avg_cart_price
    FROM
        customers
    WHERE
        event_type = 'cart'
    GROUP BY
        user_id
    HAVING AVG(price) BETWEEN 26 AND 43
) AS subquery;
"""


def get_df():
    chunks = []
    
    total_rows = pd.read_sql(total_rows_query, engine).iloc[0, 0]
    
    for chunk in tqdm(pd.read_sql(query, engine, chunksize=10000), total=total_rows//10000, unit='chunk'):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    return df


def avg_box_plot(data):
    """
    plots the avg price per user
    """


    data['avg_cart_price'] = pd.to_numeric(data['avg_cart_price'], errors='coerce')
    
    plt.figure(figsize=(10, 6))
    plt.boxplot(data['avg_cart_price'], vert=False, widths=0.5,
                    boxprops=dict(facecolor='lightblue', edgecolor='black'),
                    flierprops=dict(marker='D', markersize=8, markerfacecolor='lightgray', markeredgecolor='none'),
                    patch_artist=True, whis=0.2)
    
    plt.xticks(np.arange(int(data['avg_cart_price'].min()), int(data['avg_cart_price'].max()) + 1, step=2))
    plt.yticks([])
    plt.grid(True, axis='y', alpha=0.3)
    plt.tight_layout()

    plt.savefig('boxplot_2.png')
    plt.close()


if __name__ == "__main__":
    start_time = time.time()

    try:
        df = pd.read_sql(query, engine)

       
        #df = get_df()
        print(df)
        print()
        pd.set_option('display.float_format', '{:.2f}'.format)

        print(df.describe())
        avg_box_plot(df)
        
    except Exception as error:
        print(f"An error occurred: {error}")

    print("Execution time:", time.time() - start_time)