import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pytz
import numpy as np

# Load environment variables
load_dotenv()

# MySQL connection details from .env
mysql_ip = os.getenv("DB_HOST")
mysql_port = os.getenv("DB_PORT")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
source_table = "dividend_rawrawdata"
target_table = "dividend_yield"

# Create the connection string and engine
connection_string = f"mysql+pymysql://{user}:{password}@{mysql_ip}:{mysql_port}/{database}"

def create_mysql_engine():
    """Creates and returns a MySQL engine connection."""
    return create_engine(connection_string)

def convert_to_gregorian(minguo_date):
    """Converts Minguo date format (e.g., '102年10月24日') to Gregorian format."""
    try:
        minguo_date = minguo_date.strip()
        year_str, rest = minguo_date.split("年", 1)
        year = int(year_str) + 1911
        return f"{year}年{rest}"
    except Exception as e:
        print(f"日期轉換錯誤: {minguo_date}, 錯誤: {e}")
        return None

def process_and_store_data():
    """Fetches, processes, and stores data into the target MySQL table."""
    engine = create_mysql_engine()

    # Truncate the target table before inserting new data
    with engine.connect() as connection:
        connection.execute(f"TRUNCATE TABLE {target_table}")

    # Query data from the source table
    
    query = f"""
    SELECT ex_dividend_date, 
           stock_code, 
           CAST(pre_ex_dividend_close_price AS DECIMAL(10, 2)) AS pre_ex_dividend_close_price,
           CAST(rights_dividend_value AS DECIMAL(10, 4)) AS rights_dividend_value
    FROM {source_table}
    WHERE (rights_dividend_value != '-') and (rights_dividend_value != 0)
    and (dropa IS NOT NULL AND dropb IS NOT NULL AND pre_ex_dividend_close_price IS NOT NULL)   
    and (pre_ex_dividend_close_price != '-')
 
    """
    try:
        # Load data into a DataFrame
        data = pd.read_sql(query, con=engine)

        # Add a new column for dividend_yield
        data['dividend_yield'] = round((data['rights_dividend_value'] / data['pre_ex_dividend_close_price'] * 100), 3)

        # Clean and convert the date column
        # 移除多餘的空白字元
        data["ex_dividend_date"] = data["ex_dividend_date"].apply(lambda x: x.strip() if isinstance(x, str) else x)

        # 定義正確的日期格式
        data["ex_dividend_date"] = pd.to_datetime(data["ex_dividend_date"], format="%Y/%m/%d", errors='coerce')

        data.replace([np.inf, -np.inf, np.nan], None, inplace=True)
        # Filter out rows with invalid dates
        valid_data = data.dropna(subset=["ex_dividend_date"])

        # Store the processed data back to MySQL
        valid_data.to_sql(name=target_table, con=engine, if_exists='append', index=False)

        print("Data processed and stored successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")

# Airflow DAG settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4, 19, 0, tzinfo=pytz.timezone('Asia/Taipei')),  # Start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dividend_yield_calculation',
    default_args=default_args,
    schedule_interval='30 19 * * 1-5',  # Run daily at 19:00 Taipei time
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='process_and_store_dividend_data',
        python_callable=process_and_store_data
    )
