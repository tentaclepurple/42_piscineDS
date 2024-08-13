from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
from tqdm import tqdm


load_dotenv(dotenv_path="../.env")

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres'
DB_PORT = '5432'

query = "SELECT event_type FROM customers"

total_rows_query = "SELECT COUNT(*) FROM customers"

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'


def get_event_type_data():
    """Get the event type data from the database"""
    
    engine = create_engine(DATABASE_URL)
    chunks = []
    
    total_rows = pd.read_sql(total_rows_query, engine).iloc[0, 0]
    
    for chunk in tqdm(pd.read_sql(query, engine, chunksize=10000), total=total_rows//10000, unit='chunk'):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    return df


def plot_pie_chart(data):
    """Plot a pie chart of the event type distribution"""

    event_counts = data['event_type'].value_counts()
    plt.figure(figsize=(8, 8))
    plt.pie(event_counts, labels=event_counts.index, autopct='%1.1f%%', startangle=140)
    plt.title('Distribution of Event Types')
    plt.axis('equal')
    plt.savefig('pie_chart_event_types.png')
    print("Pie chart saved as pie_chart_event_types.png")
    plt.close()


if __name__ == "__main__":

    # Crear la URL de la base de datos

    # Crear el motor de SQLAlchemy

    try:
        event_type_df = get_event_type_data()
        plot_pie_chart(event_type_df)

    except Exception as error:
        print(f"An error occurred: {error}")
