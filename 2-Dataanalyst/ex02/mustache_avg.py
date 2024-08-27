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
        SUM(price) AS total_price
    FROM 
        customers2
    WHERE 
        event_type = 'purchase'
    GROUP BY 
        user_id, 
        user_session
)
SELECT 
    user_id,
    AVG(total_price) AS avg_purchase_price
FROM 
    session_totals
GROUP BY 
    user_id
ORDER BY 
    user_id;
"""


def avg_box_plot(data):
    """
    plots the avg price per user
    """

    data['avg_purchase_price'] = pd.to_numeric(data['avg_purchase_price'], errors='coerce')
    
    plt.figure(figsize=(10, 6))
    plt.boxplot(data['avg_purchase_price'], vert=False, widths=0.5,
                    boxprops=dict(facecolor='lightblue', edgecolor='black'),
                    flierprops=dict(marker='o', markersize=4, markerfacecolor='black', markeredgecolor='none'),
                    patch_artist=True, whis=0.2)
    
    plt.xticks(np.arange(int(data['avg_purchase_price'].min()), int(data['avg_purchase_price'].max()) + 1, step=100))
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
