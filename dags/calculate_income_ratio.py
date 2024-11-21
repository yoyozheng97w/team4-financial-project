import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import numpy as np

# 加載 .env 文件中的環境變量
load_dotenv()

# 從環境變量中構建 SQLAlchemy 的連接 URL
db_url = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Function to calculate income ratios
def compute_income_ratios(**kwargs):
    # Create database connection
    engine = create_engine(db_url)

    # Fetch data from MySQL
    query = "SELECT * FROM income_statement"
    df = pd.read_sql(query, con=engine)

    # Clean data: replace '--' with NaN and convert to numeric
    df['Operating Revenue'] = pd.to_numeric(df['Operating Revenue'], errors='coerce')
    df['Operating Cost'] = pd.to_numeric(df['Operating Cost'], errors='coerce')
    df['Operating Expenses'] = pd.to_numeric(df['Operating Expenses'], errors='coerce')
    df['Net Profit (Loss) for the Period'] = pd.to_numeric(df['Net Profit (Loss) for the Period'], errors='coerce')



# 确保分母不为零且数据有效
    df['Gross Profit'] = df['Operating Revenue'] - df['Operating Cost']

# 确保 'Operating Revenue' 不为零且有效
    df['Gross Profit Margin'] = np.where(
    (df['Operating Revenue'] != 0) & (df['Operating Revenue'].notna()), 
    df['Gross Profit'] / df['Operating Revenue'] * 100, 
    np.nan
)

    df['Operating Profit Margin'] = np.where(
    (df['Operating Revenue'] != 0) & (df['Operating Revenue'].notna()), 
    df['Operating Profit (Loss)'] / df['Operating Revenue'] * 100, 
    np.nan
)

    df['Net Profit Margin'] = np.where(
    (df['Operating Revenue'] != 0) & (df['Operating Revenue'].notna()), 
    df['Net Profit (Loss) for the Period'] / df['Operating Revenue'] * 100, 
    np.nan
)

    df['Earnings Per Share'] = df['Basic Earnings Per Share (Currency Unit)']

    # Replace inf values with NaN to avoid database issues
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Drop rows with NaN in key calculated columns
    df = df.dropna(subset=['Gross Profit', 'Gross Profit Margin', 'Operating Profit Margin', 'Net Profit Margin', 'Earnings Per Share'])

    # Create new table with ratios
    income_ratios = df[['Company ID', 'Year', 'Quarter', 'Gross Profit', 'Gross Profit Margin', 'Operating Profit Margin', 'Net Profit Margin', 'Earnings Per Share']]

    # Define English column names for the new table
    income_ratios.columns = ['Company ID', 'Year', 'Quarter', 'Gross Profit', 'Gross Profit Margin', 'Operating Profit Margin', 'Net Profit Margin', 'Earnings Per Share']

    # Save to new table in MySQL
    income_ratios.to_sql('income_ratio', con=engine, if_exists='replace', index=False)
    print("Income ratios successfully calculated and stored!")

# Airflow DAG setup
default_args = {
    'start_date': datetime(2023, 10, 20),
    'retries': 1,
}

with DAG('calculate_income_ratios_dag',
         default_args=default_args,
         schedule_interval='0 1 * * 1-5',
         catchup=False) as dag:

    compute_ratios = PythonOperator(
        task_id='compute_income_ratios',
        python_callable=compute_income_ratios
    )
