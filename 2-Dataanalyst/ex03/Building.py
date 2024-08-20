from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
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
WITH orders_by_client AS (
    SELECT user_id, COUNT(DISTINCT user_session) AS order_count

    FROM (
        SELECT DISTINCT user_id, user_session
        FROM customers2
        WHERE event_type = 'purchase'
    ) AS pedidos_unicos
    GROUP BY user_id
),
all_clients AS (
    SELECT DISTINCT user_id
    FROM customers2
),
clients_with_orders AS (
    SELECT 
        CASE
            WHEN order_count
             IS NULL THEN '0'
            WHEN order_count
             BETWEEN 1 AND 8 THEN '1 - 8'
            WHEN order_count
             BETWEEN 9 AND 16 THEN '9 - 16'
            WHEN order_count
             BETWEEN 17 AND 23 THEN '17 - 24'
            WHEN order_count
             BETWEEN 24 AND 31 THEN '25 - 31'
            ELSE '32+'
        END AS order_range
    FROM all_clients
    LEFT JOIN orders_by_client USING (user_id)
)
SELECT 
    order_range,
    COUNT(*) AS client_number
FROM clients_with_orders
GROUP BY order_range
ORDER BY 
    CASE order_range
        WHEN '0' THEN 1
        WHEN '1 - 8' THEN 2
        WHEN '9 - 16' THEN 3
        WHEN '17 - 24' THEN 4
        WHEN '25 - 31' THEN 5
        ELSE 6
    END;
"""

query2 = """
SELECT 
    CASE 
        WHEN total_amount > 0 AND total_amount <= 50 THEN 0
        WHEN total_amount > 50 AND total_amount <= 100 THEN 50
        WHEN total_amount > 100 AND total_amount <= 150 THEN 100
        WHEN total_amount > 150 AND total_amount <= 200 THEN 150
        ELSE 200
    END AS amount_spent,
    COUNT(*) AS client_amount
FROM 
    (
    SELECT 
        user_id, 
        SUM(price) AS total_amount
    FROM 
        customers2
    WHERE 
        event_type = 'purchase'
    GROUP BY 
        user_id
    ) AS buys_per_user
GROUP BY 
    amount_spent
ORDER BY 
    amount_spent;
"""


def buildings(df, df2):
    """
    Print the description of the DataFrame and save boxplots.
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

    df_filtered = df[df['order_range'] != '0']

    positions = np.linspace(4, 35.5, len(df_filtered))

    bars = ax1.bar(positions, df_filtered['client_number'], width=(35.5 - 4) / len(df_filtered) * 0.8, align='center')

    ax1.set_title('Number of Clients by Order Range (Excluding 0 orders)')
    ax1.set_xlabel('frequency')
    ax1.set_ylabel('customers (log scale)')

    ax1.set_yscale('log')
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f'{int(y):,}'))

    ax1.set_xticks([0, 10, 20, 30])
    ax1.set_xticklabels(['0', '10', '20', '30'])

    ax1.grid(axis='both', linestyle='--', alpha=0.7)

    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)

    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}', ha='center', va='bottom')


    x_labels = df2['amount_spent'].astype(str)

    bar_width = 0.8
    
    x_labels = ['0-50', '50-100', '100-150', '150-200', '200+']

    x_positions = range(len(x_labels))

    ax2.bar(x_positions, df2['client_amount'], color='skyblue', width=bar_width)


    ax2.set_ylabel('customers')
    ax2.set_xlabel('monetary value in Altairians')
    ax2.set_title('Number of Clients by Monetary Value')
    
    ax2.set_xticks(x_positions)
    ax2.set_xticklabels(x_labels)

    plt.tight_layout()

    plt.savefig('bar_chart_buildings.png')
    plt.close()


if __name__ == "__main__":
    start_time = time.time()

    #try:
    df = pd.read_sql(query, engine)
    df2 = pd.read_sql(query2, engine)
    print("df 1\n", df)
    print()
    print("df 2\n", df2)
    buildings(df, df2)

        
"""     except Exception as error:
        print(f"An error occurred: {error}")

    print("Execution time:", time.time() - start_time) """