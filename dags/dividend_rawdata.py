import os
from dotenv import load_dotenv
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import traceback
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time
from requests.exceptions import RequestException
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import timezone

# Load environment variables
load_dotenv()

def get_twse_dividend_history_for_today():
    try:
        # Get today's date in YYYYMMDD format
        today = datetime.today().strftime('%Y%m%d')
        url = f"https://www.twse.com.tw/exchangeReport/TWT49U?response=html&strDate={today}&endDate={today}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }

        # Retry mechanism
        retry_count = 3
        for i in range(retry_count):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                print("Successfully fetched HTML content.")

                # Log HTML response to a file
                with open('/tmp/twse_response.html', 'w', encoding='utf-8') as f:
                    f.write(response.text)

                # Parse HTML with BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table')

                if table is None:
                    error_message = "No table found in the HTML response."
                    log_error_to_mysql(error_message)
                    return None

                df = pd.read_html(str(table))[0]
                return df if not df.empty else None

            except RequestException as e:
                print(f"Attempt {i+1} failed: {str(e)}")
                time.sleep(5)

        log_error_to_mysql("No table found in the HTML response after retries.")
        return None

    except Exception as e:
        error_message = f"Failed to get TWSE dividend history for today. Error: {str(e)}"
        log_error_to_mysql(error_message)
        return None

def log_error_to_mysql(error_message: str):
    try:
        connection_string = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        engine = create_engine(connection_string)

        error_data = pd.DataFrame({
            'error_message': [error_message],
            'timestamp': [datetime.now()],
            'table_name': ['dividend_rawdata']
        })

        error_data.to_sql(name='error_log', con=engine, if_exists='append', index=False)
        print(f"Error logged: {error_message}")
    except Exception as e:
        print(f"Failed to log error to MySQL. Error: {str(e)}")

def insert_into_mysql(df: pd.DataFrame):
    try:
        df.columns = [
            'ex_dividend_date', 'stock_code', 'stock_name', 'pre_ex_dividend_close_price',
            'ex_dividend_reference_price', 'rights_dividend_value', 'rights_or_dividend',
            'upper_limit_price', 'lower_limit_price', 'opening_reference_price',
            'dividend_deduction_reference_price', 'detailed_info', 'latest_report_period',
            'latest_report_net_value', 'latest_report_earnings'
        ]

        connection_string = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        engine = create_engine(connection_string)

        df.to_sql(name='dividend_rawdata', con=engine, if_exists='append', index=False)
        print("Data inserted successfully.")
    except SQLAlchemyError as e:
        log_error_to_mysql(f"Failed to insert data. Error: {str(e)}")
    except Exception as e:
        log_error_to_mysql(f"Unexpected error: {traceback.format_exc()}")

def fetch_and_insert_data():
    try:
        dividend_history = get_twse_dividend_history_for_today()
        if dividend_history is not None:
            insert_into_mysql(dividend_history)
        else:
            print("No dividend data found for today.")
    except Exception as e:
        log_error_to_mysql(f"Failed to fetch and insert data. Error: {str(e)}")

# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 27, tzinfo=timezone('Asia/Taipei')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_twse_dividend_data',
    default_args=default_args,
    description='Fetch and insert daily TWSE dividend data',
    schedule_interval='0 19 * * 1-5',
    catchup=True,
)

fetch_and_insert_task = PythonOperator(
    task_id='fetch_and_insert_data',
    python_callable=fetch_and_insert_data,
    dag=dag,
)
