from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import time
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import seaborn as sns


load_dotenv(dotenv_path="../.env")

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres'
DB_PORT = '5432'

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(DATABASE_URL)

query = """
SELECT user_id, COUNT(*) AS purchases
FROM customers2
WHERE event_type = 'purchase'
GROUP BY user_id
HAVING COUNT(*) < 30
ORDER BY purchases DESC;
"""


def ft_clusters(df):
    '''
    Calculate and plot clusters and inertia
    '''
    
    wss = []
    for k in range(1, 11):
        kmeans = KMeans(n_clusters=k, random_state=0, n_init=10).fit(df)
        wss.append(kmeans.inertia_)

    plt.plot(range(1, 11), wss)
    plt.xlabel("Number of clusters")
    plt.title("The Elbow Method")

    plt.tight_layout()

    plt.savefig('elbowB.png')
    plt.close()
    
    
if __name__ == "__main__":
    start_time = time.time()

    try:
        df = pd.read_sql(query, engine)
        ft_clusters(df)

        
    except Exception as error:
        print(f"An error occurred: {error}")

    print("Execution time:", time.time() - start_time)