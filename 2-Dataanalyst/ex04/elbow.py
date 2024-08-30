from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
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
ORDER BY purchases DESC;
"""


def ft_clusters(df):
    '''
    Calculate and plot clusters and inertia
    '''
    
    pd.set_option('display.float_format', lambda x: '%.2f' % x)

    print(f"DF base: \n {df}")
    print()
    print(f"DF base describe: \n {df.describe()}")
    print()
    
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df[['purchases']])

    wss = []
    for k in range(1, 11):
        kmeans = KMeans(n_clusters=k, random_state=0, n_init=10).fit(df_scaled)
        wss.append(kmeans.inertia_)
    
    plt.plot(range(1, 11), wss)
    plt.xlabel("Number of clusters")
    plt.title("The Elbow Method")
    plt.tight_layout()
    plt.savefig('elbow.png')
    plt.close()

    kmeans = KMeans(n_clusters=2, random_state=0)
    clusters = kmeans.fit_predict(df_scaled)
    df['2clusters'] = clusters
    
    cluster_summary = df.groupby('2clusters').describe()
    print(cluster_summary['purchases'].T)
    print()
    
    kmeans_3 = KMeans(n_clusters=3, random_state=0)
    clusters_3 = kmeans_3.fit_predict(df_scaled)
    df['3clusters'] = clusters_3
    
    cluster_summary_3 = df.groupby('3clusters').describe()
    print(cluster_summary_3['purchases'].T)
    print()

    ax = df[df['2clusters'] == 0].boxplot(column='purchases', grid=False, vert=False, showfliers=False)
    plt.title('Distribution with 2 clusters - Cluster 0')
    plt.xlabel('Cluster 0')
    plt.ylabel('')
    ax.set_yticklabels([])
    plt.tight_layout()
    plt.savefig('2_clusters_boxplot_cluster_0.png')
    plt.close()
    
    ax = df[df['2clusters'] == 1].boxplot(column='purchases', grid=False, vert=False, showfliers=False)
    plt.title('Distribution with 2 clusters - Cluster 1')
    plt.xlabel('Cluster 1')
    plt.ylabel('')
    ax.set_yticklabels([])
    plt.tight_layout()
    plt.savefig('2_clusters_boxplot_cluster_1.png')
    plt.close()
    
    ax = df[df['3clusters'] == 0].boxplot(column='purchases', grid=False, vert=False, showfliers=False)
    plt.title('Distribution with 2 clusters - Cluster 0')
    plt.xlabel('Cluster 0')
    plt.ylabel('')
    ax.set_yticklabels([])
    plt.tight_layout()
    plt.savefig('3_clusters_boxplot_cluster_0.png')
    plt.close()
    
    ax = df[df['3clusters'] == 1].boxplot(column='purchases', grid=False, vert=False, showfliers=False)
    plt.title('Distribution with 3 clusters - Cluster 1')
    plt.xlabel('Cluster 1')
    plt.ylabel('')
    ax.set_yticklabels([])
    plt.tight_layout()
    plt.savefig('3_clusters_boxplot_cluster_1.png')
    plt.close()
    
    ax = df[df['3clusters'] == 2].boxplot(column='purchases', grid=False, vert=False, showfliers=False)
    plt.title('Distribution with 3 clusters - Cluster 2')
    plt.xlabel('Cluster 2')
    plt.ylabel('')
    ax.set_yticklabels([])
    plt.tight_layout()
    plt.savefig('3_clusters_boxplot_cluster_2.png')
    plt.close()
    
    
if __name__ == "__main__":
    start_time = time.time()

    try:
        df = pd.read_sql(query, engine)
        ft_clusters(df)
        
    except Exception as error:
        print(f"An error occurred: {error}")

    print("Execution time:", time.time() - start_time)
