from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import time
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


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
    us.purchase_count::float / NULLIF(us.total_events, 0) AS purchase_ratio,
    CASE 
        WHEN EXTRACT(DAY FROM NOW() - us.first_event) <= 30 THEN 'new_customer'
        WHEN EXTRACT(DAY FROM NOW() - us.last_event) > 90 THEN 'inactive_customer'
        WHEN us.total_spent >= 1000 THEN 'platinum'
        WHEN us.total_spent >= 500 THEN 'gold'
        WHEN us.total_spent >= 100 THEN 'silver'
        ELSE 'bronze'
    END AS customer_segment
FROM
    user_stats us
"""


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


def analyze_segments(df):
    
    df_normalized = preprocess_data(df)
    
    k = 0
    wss = []
    for k in range(1, 11):
        kmeans = KMeans(n_clusters=k, random_state=0, n_init=10).fit(df_normalized)
        wss.append(kmeans.inertia_)
    print(f"K = {k}")  

    plt.plot(range(1, 11), wss)
    plt.xlabel("Number of clusters")
    plt.title("The Elbow Method")

    plt.tight_layout()

    plt.savefig('elbowB.png')
    plt.close()
    
    
    # Análisis de segmentos
    segment_analysis = df.groupby('customer_segment').agg({
        'purchase_count': 'mean',
        'total_events': 'mean',
        'avg_purchase_price': 'mean',
        'total_spent': 'mean',
        'days_since_last_event': 'mean',
        'customer_lifetime_days': 'mean',
        'purchase_ratio': 'mean'
    }).round(2)
    
    
    
    
    print("Análisis de Segmentos de Clientes:")
    print(segment_analysis)
    
    # Gráfico de distribución de segmentos
    segment_distribution = df['customer_segment'].value_counts()
    plt.figure(figsize=(10, 6))
    segment_distribution.plot(kind='bar')
    plt.title('Distribución de Segmentos de Clientes')
    plt.xlabel('Segmento')
    plt.ylabel('Número de Clientes')
    plt.tight_layout()
    plt.savefig('segment_distribution.png')
    plt.close()
    
    return df

if __name__ == "__main__":
    start_time = time.time()

    try:
        df = pd.read_sql(query, engine)
        df_analyzed = analyze_segments(df)
        
        
    except Exception as error:
        print(f"Ocurrió un error: {error}")

    print("Tiempo de ejecución:", time.time() - start_time)