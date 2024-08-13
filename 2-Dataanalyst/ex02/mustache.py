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
    event_type, price
FROM
    customers
WHERE
    event_type = 'purchase'
ORDER BY
    price
"""

total_rows_query = """
SELECT COUNT(*)
FROM
    customers
WHERE
    event_type = 'purchase'
"""


def get_df():
    chunks = []
    
    total_rows = pd.read_sql(total_rows_query, engine).iloc[0, 0]
    
    for chunk in tqdm(pd.read_sql(query, engine, chunksize=10000), total=total_rows//10000, unit='chunk'):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    return df


def describe_and_box_plots(df):
    """
    Print the description of the DataFrame and save boxplots.
    """
    pd.set_option('display.float_format', '{:.2f}'.format)
    print(df.describe())

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

    boxes = ax1.boxplot(df['price'], vert=False, widths=0.5,
                        flierprops=dict(marker='D', markersize=4, markerfacecolor='black', markeredgecolor='none'),
                        patch_artist=True)
    ax1.set_yticks([])
    ax1.set_xlabel("Price")


    boxprops = dict(facecolor='green', edgecolor='black')
    medianprops = dict(linestyle='-', linewidth=2, color='black')
    ax2.boxplot(df['price'], vert=False, widths=0.5, notch=True,
                boxprops=boxprops, medianprops=medianprops, showfliers=False,
                patch_artist=True)
    ax2.set_yticks([])
    ax2.set_xlabel("Price")

    plt.tight_layout()
    plt.savefig('boxplot_1.png')
    plt.close()   


if __name__ == "__main__":
    start_time = time.time()

    try:
        #df = pd.read_sql(query, engine)
        df = get_df()

        describe_and_box_plots(df)
        
    except Exception as error:
        print(f"An error occurred: {error}")

    print("Execution time:", time.time() - start_time)