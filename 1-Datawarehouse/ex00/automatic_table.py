import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
import sqlalchemy
import threading
import time

load_dotenv(dotenv_path="../.env")

DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres'
DB_PORT = '5432'


def start_timer():
    """Start a timer that prints the elapsed time every second"""

    global start_time
    while not stop_timer:
        elapsed_time = time.time() - start_time
        print(f"Elapsed time: {elapsed_time:.2f} secs", end='\r')
        time.sleep(1)


def load(path, tableName):
    """Load CSV data into a table in the database"""
    print(f"Loading {path} into {tableName} table")
    try:
        DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

        engine = create_engine(DATABASE_URL)
        inspector = sqlalchemy.inspect(engine)
        
        if inspector.has_table(tableName):
            print(f"Table {tableName} already exists")
        else:
            print(f"Table {tableName} doesn't exist, creating...")
            data = pd.read_csv(path)
            data_types = {
                "event_time": sqlalchemy.DateTime(),
                "event_type": sqlalchemy.types.String(length=255),
                "product_id": sqlalchemy.types.Integer(),
                "price": sqlalchemy.types.Float(),
                "user_id": sqlalchemy.types.BigInteger(),
                "user_session": sqlalchemy.types.UUID(as_uuid=True)
            }
            data.to_sql(tableName, engine, index=False, dtype=data_types)
            print(f"Table {tableName} created")

        engine.dispose()
    except Exception as error:
        print(f"An error occurred: {error}")


def get_folder_files(folder: str) -> list:
    """Get the files in a folder"""
    files = os.listdir(folder)
    files = [folder + '/' + file for file in files]
    return files


def get_file_names(files: list) -> list:
    """Get the names of the files"""
    names = [file.replace('../subject/customer/', '').replace('.csv', '') for file in files]
    return(names)


if __name__ == "__main__":
    start_time = time.time()
    stop_timer = False
    timer_thread = threading.Thread(target=start_timer)
    timer_thread.start()
    
    try:
        folder = "../subject/customer"
        files = get_folder_files(folder)
        names = get_file_names(files)
        
        for csv_file, table_name in zip(files, names):
            if not csv_file.endswith('.csv'):
                continue
            load(csv_file, table_name)

        stop_timer = True
        timer_thread.join()
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\nElapsed time: {elapsed_time:.2f} secs")
        
    except Exception as e:
        stop_timer = True
        timer_thread.join()
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\nElapsed time: {elapsed_time:.2f} secs")
        print(e)
