from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import seaborn as sns
import time
from tqdm import tqdm
import threading
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score


load_dotenv(dotenv_path="../.env")

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres'
DB_PORT = '5432'

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(DATABASE_URL)


""" Calculate key statistics such as first and last purchase,
    total number of purchases,
    days since last purchase,
    and customer tenure.

    Categorize customers into mutually exclusive 
    segments based on their purchasing behavior:

    - New Customers: Less than 30 days since first purchase.
    - Inactive Customers: More than 90 days without purchase.
    - Platinum: 10 or more purchases and purchase in the last 30 days.
    - Gold: 5 or more purchases and purchase in the last 60 days.
    - Silver: 2 or more purchases and purchase in the last 90 days.
    - Bronze: All other active customers. """

query = """
WITH max_date AS (
    SELECT MAX(event_time) AS latest_date
    FROM customers2
),
user_stats AS (
    SELECT
        user_id,
        MIN(event_time) AS first_purchase,
        MAX(event_time) AS last_purchase,
        COUNT(DISTINCT event_time) AS purchase_count,
        EXTRACT(DAY FROM (SELECT latest_date FROM max_date) - MAX(event_time)) AS days_since_last_purchase,
        EXTRACT(DAY FROM (SELECT latest_date FROM max_date) - MIN(event_time)) AS customer_age_days
    FROM
        customers2
    WHERE
        event_type = 'purchase'
    GROUP BY
        user_id
)
SELECT
    us.*,
    CASE 
        WHEN customer_age_days <= 30 THEN 'new_customer'
        WHEN days_since_last_purchase > 90 THEN 'inactive_customer'
        WHEN purchase_count >= 10 AND days_since_last_purchase <= 30 THEN 'platinum'
        WHEN purchase_count >= 5 AND days_since_last_purchase <= 60 THEN 'gold'
        WHEN purchase_count >= 2 AND days_since_last_purchase <= 90 THEN 'silver'
        ELSE 'bronze'
    END AS customer_segment
FROM
    user_stats us;
 """

def barplot(df):
    """
    barplot of customer segments.
    """    
    category_names = {
        'inactive_customer': 'Inactive Customers',
        'new_customer': 'New Customers',
        'bronze': 'Bronze',
        'silver': 'Silver',
        'gold': 'Gold',
        'platinum': 'Platinum',
        'loyal': 'Loyal Customers'
    }

    df['segment_count_w/loyal'] = df['customer_segment'].replace({
        'silver': 'loyal',
        'gold': 'loyal',
        'platinum': 'loyal'
    })
    segment_counts = df['customer_segment'].value_counts()
    segment_counts_loyal = df['segment_count_w/loyal'].value_counts()
    
    df_segment_counts = pd.DataFrame({'Segment': segment_counts.index, 'Count': segment_counts.values})
    df_segment_counts_loyal = pd.DataFrame({'Segment': segment_counts_loyal.index, 'Count': segment_counts_loyal.values})

    df_segment_counts['Segment'] = df_segment_counts['Segment'].map(category_names)
    df_segment_counts_loyal['Segment'] = df_segment_counts_loyal['Segment'].map(category_names)

    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(16, 6))
    
    sns.barplot(x='Count', y='Segment', data=df_segment_counts, hue='Segment', palette='husl', ax=axes[0])
    axes[0].set_title('Customer Segments')
    axes[0].set_xlabel('Number of customers')
    axes[0].set_ylabel('')
    for container in axes[0].containers:
        axes[0].bar_label(container, fmt='%d', padding=3)

    sns.barplot(x='Count', y='Segment', data=df_segment_counts_loyal, hue='Segment', palette='pastel', ax=axes[1])
    axes[1].set_title('Segments With Loyal Customers Grouped')
    axes[1].set_xlabel('Number of customers')
    axes[1].set_ylabel('')
    
    for container in axes[1].containers:
        axes[1].bar_label(container, fmt='%d', padding=3)
        
    for ax in axes:
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig('customer_segment_counts.png')
    plt.close() 
        

def recency_vs_frequency(df):
    """ 
    scatterplot of purchase frequency vs days since last purchase for each customer segment.
    """
    grouped_segments = df.groupby('customer_segment').agg({
        'days_since_last_purchase': 'median',
        'purchase_count': 'median'
    }).reset_index()
    
    colors = ['darkgoldenrod', 'dodgerblue', 'palevioletred', 'green', 'purple', 'teal']

    color_dict = {segment: color for segment, color in zip(grouped_segments['customer_segment'], colors)}

    
    plt.scatter(
        grouped_segments['days_since_last_purchase'],  # Recency
        grouped_segments['purchase_count'],  # Frequency
        s=grouped_segments['purchase_count'] * 100,  # dot size based on frequency
        c=grouped_segments['customer_segment'].map(color_dict),  # Color based on segment
        alpha=0.6  # transparency
    )
    
    for i in range(len(grouped_segments)):
        plt.annotate(
            f'{grouped_segments["customer_segment"].iloc[i]}', 
            (grouped_segments['days_since_last_purchase'].iloc[i], grouped_segments['purchase_count'].iloc[i]),
            fontsize=10,
            ha='center'
        )
    
    plt.xlabel('Median Recency (days)')
    plt.ylabel('Median Frequency')
    plt.title('Recency vs Frequency by Customer Segment')
        
    plt.savefig('recency_vs_frequency.png')
    plt.close()
    
    
def kmeans_clustering(df):
    
    
    features = ['purchase_count', 'customer_age_days']
    cluster_number = 2

    # Escala las características para que tengan media 0 y varianza 1
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df[features])
    
    wss = []
    for k in range(1, 11):
        kmeans = KMeans(n_clusters=k, random_state=0, n_init=10).fit(X_scaled)
        wss.append(kmeans.inertia_)
    
    plt.plot(range(1, 11), wss)
    plt.xlabel("Number of clusters")
    plt.title("The Elbow Method")
    plt.tight_layout()
    plt.savefig('elbowmethod.png')
    plt.close()

    kmeans = KMeans(n_clusters=cluster_number, random_state=42)
    df['cluster'] = kmeans.fit_predict(X_scaled)
    
    print(df.head())

    # Añadir los centroides a la visualización
    centroids = kmeans.cluster_centers_

    # Desescalamos los centroides para visualizarlos en la escala original
    centroids_original = scaler.inverse_transform(centroids)

    # Crear el gráfico de dispersión
    plt.figure(figsize=(10, 8))
    
    # Colores para los clusters
    colors = ['turquoise', 'gold', 'salmon', 'teal', 'hotpink', 'lightgreen']  

    for i in range(cluster_number): 
        plt.scatter(
            df[df['cluster'] == i]['purchase_count'], 
            df[df['cluster'] == i]['customer_age_days'], 
            s=100,  # Tamaño del punto
            c=colors[i],  # Color basado en el cluster
            label=f'Cluster {i}',
            alpha=0.3
        )
    
    # Añadir los centroides al gráfico
    plt.scatter(centroids_original[:, 0], centroids_original[:, 1], 
                s=300, c='navy', marker='X', label='Centroids')

    # Etiquetas y título
    plt.ylabel('purchase_count')
    plt.xlabel('customer age days')
    plt.title('Customer Clusters with Centroids')
    plt.legend()
    plt.grid(True)


    plt.tight_layout()
    plt.savefig('cluster_distribution.png')
    plt.close()





def prints(df):
    print(df.head())
    print()
    print(df.info())
    print()
    print(df.describe())
    print()
    count = df['customer_segment'].value_counts()
    print(count)

def graph_thread(df):
    barplot(df)
    recency_vs_frequency(df)
    kmeans_clustering(df)

if __name__ == "__main__":
    start_time = time.time()
    
    pickle_file = 'clustering_df.pkl'

    try:

        pd.set_option('display.float_format', lambda x: '%.2f' % x)
        
        if os.path.exists(pickle_file):
            with open(pickle_file, 'rb') as file:
                df = pd.read_pickle(file)

        else:
            df = pd.read_sql(query, engine)
            
            with open(pickle_file, 'wb') as file:
                df.to_pickle(file)

        print(df.head())
        print()
        print(df.info())
        print()
        print(df.describe())
        print()
        count = df['customer_segment'].value_counts()
        print(count) 
        
        
        barplot(df)
        recency_vs_frequency(df)
        kmeans_clustering(df)


        """ print_thread = threading.Thread(target=prints, args=(df,))
        graph_thread = threading.Thread(target=graph_thread, args=(df,))        
        
        print_thread.start()
        graph_thread.start()

        print_thread.join()
        graph_thread.join() """
        
    except Exception as error:
       print(f"Ocurrió un error: {error}")

    print("Tiempo de ejecución:", time.time() - start_time)