import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from io import StringIO
import requests
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
import pendulum
import tempfile

# Load environment variables from .env
load_dotenv()

# MySQL connection details from .env
mysql_ip = os.getenv("DB_HOST")
mysql_port = os.getenv("DB_PORT")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
table_name = "stock_prices_rawdata"

COLUMN_ORDER = [
    "record_date", "stock_code", "stock_name", "transaction_volume",
    "transaction_count", "transaction_amount", "opening_price",
    "highest_price", "lowest_price", "closing_price", "price_change_symbol",
    "price_change", "last_bid_price", "last_bid_volume",
    "last_ask_price", "last_ask_volume", "pe_ratio", "additional_info"
]

def create_mysql_engine():
    connection_string = f"mysql+pymysql://{user}:{password}@{mysql_ip}:{mysql_port}/{database}?charset=utf8mb4"
    return create_engine(connection_string)


def log_error_to_mysql(error_message: str):
    """Log error message to MySQL error_log table."""
    try:
        # 建立資料庫引擎和連線
        engine = create_mysql_engine()
        connection = engine.connect()
        print("Successfully connected to MySQL.")

        # 確保獲取當前時間
        current_time = pendulum.now('Asia/Taipei')
        if current_time is None:
            current_time = pendulum.now('UTC')

        # 構建錯誤日誌數據
        error_data = pd.DataFrame({
            'error_message': [error_message],
            'timestamp': [current_time.naive()],
            'table_name': [table_name if table_name else 'unknown']
        })

        # 嘗試寫入 MySQL
        error_data.to_sql(name='error_log', con=connection, if_exists='append', index=False)
        print(f"Error logged to error_log table: {error_message}")

    except Exception as e:
        # 增加更多的 debug 輸出
        print(f"Failed to log error to MySQL. Error: {str(e)}")

    finally:
        # 確保在 finally 中關閉連接
        if 'connection' in locals() and connection is not None:
            try:
                connection.close()
                print("MySQL connection closed.")
            except Exception as e:
                print(f"Failed to close MySQL connection. Error: {str(e)}")



# DAG 定義
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 29, 10, 0, tzinfo=pendulum.timezone('UTC')), 
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG('close_prices_rawdata', default_args=default_args, schedule_interval='0 20 * * 1-5', catchup=True) as dag:

    @task
    def fetch_data_from_twse():
        try:
            date = pendulum.now('Asia/Taipei')
            url = f"http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date={date.strftime('%Y%m%d')}&type=ALL"
            response = requests.post(url, timeout=30)
            if response.status_code == 200:
                response.encoding = 'big5'
                if not response.text.strip():
                    # 正常記錄錯誤資訊
                    log_error_to_mysql(f"Data is empty for date: {date.to_date_string()}")
                    return date.isoformat(), None  # 返回 None 而不創建臨時文件
                
                # 如果有數據，才創建臨時文件
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
                with open(temp_file.name, "w", encoding="utf-8") as f:
                    f.write(response.text)
                
                return date.isoformat(), temp_file.name
            else:
                raise Exception(f"Unable to fetch data from TWSE, HTTP Status: {response.status_code}")
        except Exception as e:
            log_error_to_mysql(f"Failed to fetch data from TWSE: {str(e)}")
            raise


    @task
    def process_csv_to_dataframe(data):
        date_str, file_path = data
        date = pendulum.parse(date_str)

        if file_path is None:
            log_error_to_mysql(f"CSV data is empty for date: {date.to_date_string()}")
            return date.isoformat(), None

        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                csv_data = f.read()

            df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '})
                                                 for i in csv_data.split('\n')
                                                 if len(i.split('",')) == 17 and i[0] != '='])), header=0)
            df.insert(0, 'record_date', date.to_date_string())
            
            if len(df.columns) == 18:
                df.columns = COLUMN_ORDER
            else:
                raise ValueError("Parsing error: Column count does not match the expected 18 columns")

            columns_to_clean = ['transaction_amount', 'transaction_volume', 'transaction_count',
                                'last_bid_volume', 'last_ask_volume', 'last_bid_price', 
                                'last_ask_price', 'opening_price', 'highest_price', 
                                'lowest_price', 'closing_price', 'price_change']
            for col in columns_to_clean:
                df[col] = df[col].astype(str).str.replace(',', '').replace('--', np.nan).astype(float)

            df['price_change_symbol'] = df['price_change_symbol'].replace({'+': 'plus', '-': 'minus'})

            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            df.to_csv(temp_file.name, index=False)
            temp_file.close()
            os.remove(file_path)  # 刪除 fetch_data_from_twse 中的臨時文件

            return date.isoformat(), temp_file.name
        except Exception as e:
            log_error_to_mysql(f"Failed to process CSV to DataFrame: {str(e)}")
            raise

    @task
    def load_data_to_mysql(data):
        date_str, file_path = data
        date = pendulum.parse(date_str)

        if file_path is None:
            log_error_to_mysql(f"No new data to insert for {date.to_date_string()}")
            return

        df = pd.read_csv(file_path)
        engine = create_mysql_engine()
        connection = engine.connect()
        try:
            existing_data = pd.read_sql_query(
                f"SELECT record_date, stock_code FROM {table_name} WHERE record_date = '{date.to_date_string()}'", 
                con=connection
            )

            if not existing_data.empty:
                df = df.merge(existing_data, on=['record_date', 'stock_code'], how='left', indicator=True)
                df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])

            if df.empty:
                log_error_to_mysql(f"No new data to insert for {date.to_date_string()}")
                return

            df.to_sql(name=table_name, con=connection, if_exists='append', index=False)
        except Exception as e:
            log_error_to_mysql(f"Failed to load data to MySQL: {str(e)}")
            raise
        finally:
            connection.close()
            os.remove(file_path)  # 刪除 process_csv_to_dataframe 中的臨時文件

    data_fetched = fetch_data_from_twse()
    processed_data = process_csv_to_dataframe(data_fetched)
    load_data_to_mysql(processed_data)
