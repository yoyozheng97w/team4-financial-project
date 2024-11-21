from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import pandas as pd
from io import StringIO
import time
import random
import pymysql
from typing import Optional, List, Dict
import os
from dotenv import load_dotenv

# DAG 預設參數
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

# 資料庫設定
load_dotenv()
DB_CONFIG = {
    "host": os.getenv('DB_HOST'),
    "port": int(os.getenv('DB_PORT')),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "database": os.getenv('DB_NAME'),
    "charset":  os.getenv('DB_CHARSET'),
}

def connect_db():
    conn = pymysql.connect(**DB_CONFIG)
    conn.ping(reconnect=True)  # 確保連接是活的
    return conn

@task
def create_table() -> None:
    """建立現金流量表資料表"""
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            sql = """
            CREATE TABLE IF NOT EXISTS cashflow_statement (
                id VARCHAR(10),
                year INT,
                quarter INT,
                company_name VARCHAR(100),
                operating_cashflow DECIMAL(20, 2),
                investing_cashflow DECIMAL(20, 2),
                financing_cashflow DECIMAL(20, 2),
                exchange_rate_effect DECIMAL(20, 2),
                net_cash_increase DECIMAL(20, 2),
                cash_end_of_period DECIMAL(20, 2),
                cash_beginning_of_period DECIMAL(20, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_id_year_quarter (id, year, quarter)
            )
            """
            cursor.execute(sql)
        conn.commit()
    finally:
        conn.close()

def safe_decimal(value) -> float:
    """轉換各種輸入格式為浮點數"""
    if value == '--' or value == 'N/A' or pd.isna(value):
        return 0
    try:
        return float(value)
    except ValueError:
        return 0

@task
def get_year_quarter_list() -> List[tuple]:
    """產生年季清單"""
    year_quarter_list = []
    for year in range(113, 102, -1):
        if year == 113:
            seasons = range(1, 4)
        else:
            seasons = range(1, 5)
        for season in seasons:
            year_quarter_list.append((year, season))
    return year_quarter_list

def insert_with_retry(cursor, sql, data, max_retries=3, retry_delay=5):
    """使用重試機制的資料插入函數"""
    for attempt in range(max_retries):
        try:
            cursor.execute(sql, data)
            return True
        except pymysql.err.OperationalError as e:
            if e.args[0] == 1213 and attempt < max_retries - 1:  # Deadlock error
                time.sleep(retry_delay)
                continue
            raise
    return False

@task
def fetch_and_process_data(year_quarter: tuple) -> None:
    """擷取並處理特定年季的現金流量資料"""
    year, season = year_quarter
    
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb20'
    parameter = {
        'firstin': '1',
        'TYPEK': 'sii',
        'year': str(year),
        'season': str(season)
    }

    try:
        res = requests.post(url, data=parameter)
        res.raise_for_status()

        tables = pd.read_html(StringIO(res.text))
        
        if len(tables) > 3:
            df = tables[3]
            
            required_columns = [
                '公司 代號', '公司名稱', '營業活動之淨現金流入（流出）',
                '投資活動之淨現金流入（流出）', '籌資活動之淨現金流入（流出）',
                '匯率變動對現金及約當現金之影響', '本期現金及約當現金增加（減少）數',
                '期末現金及約當現金餘額', '期初現金及約當現金餘額'
            ]

            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"缺少欄位: {missing_columns}")

            # 分批處理資料以減少死鎖機會
            batch_size = 50
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                
                conn = connect_db()
                try:
                    with conn.cursor() as cursor:
                        for _, row in batch_df.iterrows():
                            sql = """
                            INSERT INTO cashflow_statement (
                                id, year, quarter, company_name, operating_cashflow,
                                investing_cashflow, financing_cashflow, exchange_rate_effect,
                                net_cash_increase, cash_end_of_period, cash_beginning_of_period
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                company_name = VALUES(company_name),
                                operating_cashflow = VALUES(operating_cashflow),
                                investing_cashflow = VALUES(investing_cashflow),
                                financing_cashflow = VALUES(financing_cashflow),
                                exchange_rate_effect = VALUES(exchange_rate_effect),
                                net_cash_increase = VALUES(net_cash_increase),
                                cash_end_of_period = VALUES(cash_end_of_period),
                                cash_beginning_of_period = VALUES(cash_beginning_of_period)
                            """
                            data = (
                                row['公司 代號'], year, season, row['公司名稱'],
                                safe_decimal(row['營業活動之淨現金流入（流出）']),
                                safe_decimal(row['投資活動之淨現金流入（流出）']),
                                safe_decimal(row['籌資活動之淨現金流入（流出）']),
                                safe_decimal(row['匯率變動對現金及約當現金之影響']),
                                safe_decimal(row['本期現金及約當現金增加（減少）數']),
                                safe_decimal(row['期末現金及約當現金餘額']),
                                safe_decimal(row['期初現金及約當現金餘額'])
                            )
                            
                            if not insert_with_retry(cursor, sql, data):
                                raise Exception("Maximum retry attempts reached")
                            
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise e
                finally:
                    conn.close()
                
                # 加入批次間的延遲
                time.sleep(random.uniform(1, 2))

    except Exception as e:
        raise Exception(f"處理 {year} 年第 {season} 季資料時發生錯誤: {str(e)}")
    
    # 加入延遲以避免過度請求
    time.sleep(random.uniform(2, 5))

# 建立 DAG
with DAG(
    'taiwan_stock_cashflow_etl',
    default_args=default_args,
    description='台股現金流量表 ETL 處理',
    schedule_interval='0 0 * * 1-5',  # 每天午夜執行
    catchup=False,
    max_active_tasks=3  # 限制同時執行的任務數
) as dag:
    
    create_table_task = create_table()
    year_quarter_list = get_year_quarter_list()
    
    process_tasks = fetch_and_process_data.expand(
        year_quarter=year_quarter_list
    )
    
    create_table_task >> process_tasks