import os
import requests
import pandas as pd
import time
import random
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator

# 加載 .env 文件中的環境變量
load_dotenv()

# 從環境變量中構建 SQLAlchemy 的連接 URL
db_url = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Function to fetch income statement
def get_income_statement(TYPEK, year, season, retries=3):
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
    parameter = {'firstin': '1', 'TYPEK': TYPEK, 'year': str(year), 'season': str(season)}

    for attempt in range(retries):
        try:
            res = requests.post(url, data=parameter)
            res.raise_for_status()
            tables = pd.read_html(res.text)

            if len(tables) < 4:
                raise Exception("Not enough tables found")

            df = tables[3]
            df.insert(1, 'Year', year)
            df.insert(2, 'Season', season)
            print(f"Data shape fetched: {df.shape}")
            return df
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            if attempt < retries - 1:
                wait_time = 2 ** attempt
                print(f"Retrying after {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise Exception(f"Failed to fetch data for Year {year} Season {season} after {retries} retries")

# Function to automate data fetching
def auto_fetch_data(TYPEK, start_year, start_season, **kwargs):
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
            df = get_income_statement(TYPEK, year, season)
            all_data = pd.concat([all_data, df], ignore_index=True)
        except Exception as e:
            print(f'Unable to fetch data for Year {year} Season {season}: {e}')

        # Add random sleep time to avoid frequent requests
        sleep_time = random.randint(3, 4)
        print(f'Sleeping for {sleep_time} seconds before the next request...')
        time.sleep(sleep_time)

        # Update season and year
        if season < 4:
            season += 1
        else:
            season = 1
            year += 1

    # Define the English column names with shortened names
    english_columns = [
        'Company ID', 'Year', 'Quarter', 'Company Name', 'Operating Revenue', 'Operating Cost', 
        'Gross Profit (Loss)', 'Unrealized Sales (Loss) Profit', 'Realized Sales (Loss) Profit', 
        'Net Gross Profit (Loss)', 'Operating Expenses', 'Net Other Gains and Losses', 
        'Operating Profit (Loss)', 'Non-Operating Income and Expenses', 'Net Profit (Loss) Before Tax', 
        'Income Tax Expense (Benefit)', 'Net Profit (Loss) from Continuing Operations', 
        'Profit (Loss) from Discontinued Operations', 'Profit (Loss) NCI Before Consolidation', 
        'Net Profit (Loss) for the Period', 'Other Comprehensive Income (Net)', 
        'Comprehensive Income NCI Before Consolidation', 'Total Comprehensive Income for the Period', 
        'Net Profit (Loss) Attributable to Owners of the Parent', 
        'Net Profit (Loss) Equity Under Joint Control', 'Net Profit (Loss) Attributable to NCI', 
        'Total Comprehensive Income Owners of Parent', 'Total Comprehensive Income Joint Control', 
        'Total Comprehensive Income NCI', 'Basic Earnings Per Share (Currency Unit)', 
        'Original Gains (Losses) from Bio Assets', 'Changes in Fair Value Bio Assets'
    ]

    # Save data to MySQL
    engine = create_engine(db_url)
    try:
        all_data.columns = english_columns
        all_data.to_sql('income_statement', con=engine, if_exists='replace', index=False)
        print("Data successfully loaded with English column names!")
    except Exception as e:
        print(f"Error occurred: {e}")

    # Check data count in table
    with engine.connect() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM income_statement"))
        count = result.scalar()
        print(f"Total records in income_statement table: {count}")

# Airflow DAG setup
default_args = {
    'start_date': datetime(2023, 10, 20),
    'retries': 1,
}

with DAG('fetch_financial_data_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_financial_data',
        python_callable=auto_fetch_data,
        op_kwargs={'TYPEK': 'sii', 'start_year': 103, 'start_season': 1}
    )
