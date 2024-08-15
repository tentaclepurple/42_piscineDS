from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import seaborn as sns
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
WITH pedidos_por_cliente AS (
    SELECT user_id, COUNT(DISTINCT user_session) AS num_pedidos
    FROM (
        SELECT DISTINCT user_id, user_session
        FROM customers
        WHERE event_type = 'purchase'
    ) AS pedidos_unicos
    GROUP BY user_id
),
todos_los_clientes AS (
    SELECT DISTINCT user_id
    FROM customers
),
clientes_con_pedidos AS (
    SELECT 
        CASE
            WHEN num_pedidos IS NULL THEN '0 pedidos'
            WHEN num_pedidos BETWEEN 1 AND 8 THEN '1 - 8 pedidos'
            WHEN num_pedidos BETWEEN 9 AND 16 THEN '9 - 16 pedidos'
            WHEN num_pedidos BETWEEN 17 AND 24 THEN '17 - 24 pedidos'
            WHEN num_pedidos BETWEEN 25 AND 31 THEN '25 - 31 pedidos'
            WHEN num_pedidos BETWEEN 32 AND 38 THEN '32 - 38 pedidos'
            ELSE '39+ pedidos'
        END AS rango_pedidos
    FROM todos_los_clientes
    LEFT JOIN pedidos_por_cliente USING (user_id)
)
SELECT 
    rango_pedidos,
    COUNT(*) AS num_clientes
FROM clientes_con_pedidos
GROUP BY rango_pedidos
ORDER BY 
    CASE rango_pedidos
        WHEN '0 pedidos' THEN 1
        WHEN '1 - 8 pedidos' THEN 2
        WHEN '9 - 16 pedidos' THEN 3
        WHEN '17 - 24 pedidos' THEN 4
        WHEN '25 - 31 pedidos' THEN 5
        WHEN '32 - 38 pedidos' THEN 6
        ELSE 7
    END;
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

    
    # Consulta SQL modificada para obtener los datos necesarios


    # Ejecuta la consulta y carga los resultados en un DataFrame
    df = pd.read_sql(query, engine)

    # Crear figura con dos subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))

    # Gráfico 1: Número de pedidos según la frecuencia
    order_counts = df['order_count'].value_counts().sort_index()
    sns.barplot(x=order_counts.index, y=order_counts.values, ax=ax1)
    ax1.set_title('Number of Orders According to Frequency')
    ax1.set_xlabel('Frequency')
    ax1.set_ylabel('Customers')

    # Gráfico 2: Dólares Altairianos gastados en el sitio
    # Agrupamos los gastos en intervalos
    df['spend_group'] = pd.cut(df['total_spent'], bins=[0, 50, 100, 150, 200, float('inf')],
                            labels=['0-50', '50-100', '100-150', '150-200', '200+'])
    spend_counts = df['spend_group'].value_counts().sort_index()
    sns.barplot(x=spend_counts.index, y=spend_counts.values, ax=ax2)
    ax2.set_title('Altairian Dollars Spent on the Site by Customers')
    ax2.set_xlabel('Monetary Value in ₳')
    ax2.set_ylabel('Customers')

    plt.tight_layout()
    plt.savefig('boxplot_1.png')
    plt.close()   


if __name__ == "__main__":
    start_time = time.time()

    try:
        #df = pd.read_sql(query, engine)
        df = get_df()
        print(df)

        describe_and_box_plots(df)
        
    except Exception as error:
        print(f"An error occurred: {error}")

    print("Execution time:", time.time() - start_time)