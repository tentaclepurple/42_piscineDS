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
WITH user_stats AS (
    SELECT
        user_id,
        MIN(event_time) AS first_event,
        MAX(event_time) AS last_event,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN event_time END) AS purchase_count,
        COUNT(DISTINCT event_time) AS total_events,
        AVG(CASE WHEN event_type = 'purchase' THEN price ELSE NULL END) AS avg_purchase_price,
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_spent
    FROM
        customers2
    GROUP BY
        user_id
)
SELECT
    us.*,
    EXTRACT(DAY FROM NOW() - us.last_event) AS days_since_last_event,
    EXTRACT(DAY FROM us.last_event - us.first_event) AS customer_lifetime_days,
    us.purchase_count::float / NULLIF(us.total_events, 0) AS purchase_ratio
FROM
    user_stats us;
"""


def visualize_clusters(df):
    '''
    Visualiza los clústeres en 2D utilizando PCA
    '''
    pca = PCA(n_components=3)
    df_pca = pca.fit_transform(preprocess_data(df))
    
    # Transformar los centroides a las mismas dimensiones que PCA
    centroids_pca = pca.transform(kmeans_model.cluster_centers_)
    
    df_pca = pd.DataFrame(df_pca, columns=['PCA1', 'PCA2'])
    df_pca['cluster'] = df['cluster']
    
    plt.figure(figsize=(10, 8))
    
    # Graficar los puntos
    sns.scatterplot(x='PCA1', y='PCA2', hue='cluster', data=df_pca, palette='viridis', s=100)
    
    # Añadir los centroides
    plt.scatter(centroids_pca[:, 0], centroids_pca[:, 1], s=300, c='red', marker='X', label='Centroides')
    plt.title('Visualización de Clústeres con PCA')
    plt.savefig('cluster_visual.png')


def preprocess_data(df):
    '''
    Preprocess and normalize data
    '''
    features = ['purchase_count', 'total_events', 'avg_purchase_price', 'total_spent',
                'days_since_last_event', 'customer_lifetime_days', 'purchase_ratio']
    
    df[features] = df[features].fillna(0)
    scaler = StandardScaler()
    df_scaled = pd.DataFrame(scaler.fit_transform(df[features]), columns=features)
    
    return df_scaled


def ft_clusters(df):
    '''
    Calculate and plot clusters and inertia
    '''
    
    df_scaled = preprocess_data(df)
    inertias = []
    max_clusters = 10

    for k in range(1, max_clusters + 1):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        kmeans.fit(df_scaled)
        inertias.append(kmeans.inertia_)
    
    # Plot inertia
    plt.figure(figsize=(10, 5))
    plt.plot(range(1, 11), inertias, marker='o')
    plt.xlabel('Number of clusters')
    plt.ylabel('Inertia')
    plt.title('Inertia vs Number of clusters')
    plt.grid()
    
    plt.gca().yaxis.set_major_formatter(mticker.ScalarFormatter(useOffset=False))
    plt.gca().yaxis.get_major_formatter().set_scientific(False)
    plt.gca().yaxis.set_minor_formatter(mticker.ScalarFormatter(useOffset=False))

    plt.tight_layout()

    plt.savefig('clusters_vs_inertia.png')
    plt.close()
    
    kmeans = KMeans(n_clusters=5, random_state=42, n_init=10)
    kmeans.fit(df_scaled)
    
    df['cluster'] = kmeans.labels_
    
    print(df.head())
    
    visualize_clusters(df, kmeans)


if __name__ == "__main__":
    start_time = time.time()

    try:
        df = pd.read_sql(query, engine)
        ft_clusters(df)

        
    except Exception as error:
        print(f"An error occurred: {error}")

    print("Execution time:", time.time() - start_time)