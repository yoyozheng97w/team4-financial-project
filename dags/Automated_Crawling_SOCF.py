import requests
import pandas as pd
import time
import random  # 引入随机模块
from datetime import datetime
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator

# Function to fetch cash flow statement
def get_cash_flow_statement(TYPEK, year, season, retries=3):
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb20'
    parameter = {'firstin': '1', 'TYPEK': TYPEK, 'year': str(year), 'season': str(season)}

    for attempt in range(retries):
        try:
            res = requests.post(url, data=parameter)
            res.raise_for_status()  # Check if request was successful
            tables = pd.read_html(res.text)

            if len(tables) < 4:
                raise Exception("Not enough tables found")

            df = tables[3]  # Assuming the 4th table is correct
            df.insert(1, 'Year', year)
            df.insert(2, 'Season', season)
            print(f"Data shape fetched: {df.shape}")
            return df
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            if attempt < retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Retrying after {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise Exception(f"Failed to fetch data for Year {year} Season {season} after {retries} retries")

# Function to automate data fetching
def auto_fetch_cash_flow_data(TYPEK, start_year, start_season, **kwargs):
    current_year = datetime.now().year - 1911
    current_month = datetime.now().month
    current_day = datetime.now().day

    # Determine the current season based on month and day
    if (current_month == 4 and current_day >= 1) or (current_month == 5 and current_day <= 15):
        current_season = 1
    elif (current_month == 5 and current_day >= 16) or (current_month < 8) or (current_month == 8 and current_day <= 14):
        current_season = 2
    elif (current_month == 8 and current_day >= 15) or (current_month < 11) or (current_month == 11 and current_day <= 14):
        current_season = 3
    else:
        current_season = 4

    all_data = pd.DataFrame()
    year = start_year
    season = start_season

    # Loop to fetch data
    while (year < current_year) or (year == current_year and season <= current_season):
        print(f'Fetching data for Year {year} Season {season}...')
        try:
            df = get_cash_flow_statement(TYPEK, year, season)
            all_data = pd.concat([all_data, df], ignore_index=True)
        except Exception as e:
            print(f'Unable to fetch data for Year {year} Season {season}: {e}')

        # Add random sleep time to avoid frequent requests
        sleep_time = random.randint(3, 5)  # Random sleep time between 3 to 5 seconds
        print(f'Sleeping for {sleep_time} seconds before the next request...')
        time.sleep(sleep_time)

        # Update season and year
        if season < 4:
            season += 1
        else:
            season = 1
            year += 1

    # Save data to MySQL
    engine = create_engine('mysql+pymysql://airflow:airflow@mysql:3306/financial')
    try:
        all_data.to_sql('cash_flow_statement', con=engine, if_exists='replace', index=False)
        print("Data successfully loaded!")
    except Exception as e:
        print(f"Error occurred: {e}")

    # Check data count in table
    with engine.connect() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM cash_flow_statement"))
        count = result.scalar()
        print(f"Total records in cash_flow_statement table: {count}")

# Airflow DAG setup
default_args = {
    'start_date': datetime(2023, 10, 21),
    'retries': 1,
}

with DAG('fetch_financial_data3_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_financial_data3',
        python_callable=auto_fetch_cash_flow_data,
        op_kwargs={'TYPEK': 'sii', 'start_year': 103, 'start_season': 1}
    )
