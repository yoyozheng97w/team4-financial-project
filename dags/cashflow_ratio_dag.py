from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pymysql
import pandas as pd
import logging
import os
from dotenv import load_dotenv

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    """建立資料庫連接"""
    conn = pymysql.connect(**DB_CONFIG)
    conn.ping(reconnect=True)
    return conn

@task
def create_table() -> None:
    """建立現金流量比率資料表"""
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            sql = """
            CREATE TABLE IF NOT EXISTS cashflow_ratio (
                id VARCHAR(10),
                year INT,
                quarter INT,
                company_name VARCHAR(100),
                operating_cashflow DECIMAL(20, 2),
                current_liabilities DECIMAL(20, 2),
                cashflow_ratio DECIMAL(20, 4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_id_year_quarter (id, year, quarter)
            )
            """
            cursor.execute(sql)
        conn.commit()
        logger.info("Successfully created cashflow_ratio table")
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise
    finally:
        conn.close()

@task
def get_year_quarter_list() -> list:
    """取得需要處理的年季清單"""
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            sql = """
            SELECT DISTINCT year, quarter 
            FROM cashflow_statement 
            WHERE year >= 103 
            ORDER BY year DESC, quarter DESC
            """
            cursor.execute(sql)
            return cursor.fetchall()
    finally:
        conn.close()

def safe_division(numerator: float, denominator: float) -> float:
    """安全的除法運算，避免除以零的錯誤"""
    try:
        if denominator == 0:
            return 0
        return numerator / denominator
    except:
        return 0

@task
def calculate_cashflow_ratio(year_quarter: tuple) -> None:
    """計算特定年季的現金流量比率"""
    year, quarter = year_quarter
    logger.info(f"Processing year: {year}, quarter: {quarter}")
    
    conn = connect_db()
    try:
        # 從兩個表格獲取所需數據
        sql = """
        SELECT 
            cs.id,
            cs.year,
            cs.quarter,
            cs.company_name,
            cs.operating_cashflow,
            bs.current_liabilities
        FROM cashflow_statement cs
        LEFT JOIN balance_sheet bs 
        ON cs.id = bs.stock_code 
        AND cs.year = bs.year 
        AND cs.quarter = bs.quarter
        WHERE cs.year = %s AND cs.quarter = %s
        """
        
        with conn.cursor() as cursor:
            cursor.execute(sql, (year, quarter))
            rows = cursor.fetchall()
            
            for row in rows:
                id, year, quarter, company_name, operating_cashflow, current_liabilities = row
                
                # 計算現金流量比率
                cashflow_ratio = safe_division(
                    float(operating_cashflow if operating_cashflow is not None else 0),
                    float(current_liabilities if current_liabilities is not None else 0)
                ) * 100  # 轉換為百分比
                
                # 插入或更新數據
                insert_sql = """
                INSERT INTO cashflow_ratio 
                (id, year, quarter, company_name, operating_cashflow, 
                current_liabilities, cashflow_ratio)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                company_name = VALUES(company_name),
                operating_cashflow = VALUES(operating_cashflow),
                current_liabilities = VALUES(current_liabilities),
                cashflow_ratio = VALUES(cashflow_ratio)
                """
                
                cursor.execute(insert_sql, (
                    id, year, quarter, company_name,
                    operating_cashflow, current_liabilities, cashflow_ratio
                ))
            
        conn.commit()
        logger.info(f"Successfully processed data for {year} Q{quarter}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing {year} Q{quarter}: {str(e)}")
        raise
    finally:
        conn.close()

# 建立 DAG
with DAG(
    'cashflow_ratio_calculation',
    default_args=default_args,
    description='計算公司現金流量比率',
    schedule_interval='0 2 * * 1-5',  # 每天凌晨 2 點執行
    catchup=False,
    max_active_tasks=3
) as dag:
    
    # 建立資料表
    create_table_task = create_table()
    
    # 取得年季清單
    year_quarter_list = get_year_quarter_list()
    
    # 展開計算任務
    process_tasks = calculate_cashflow_ratio.expand(
        year_quarter=year_quarter_list
    )
    
    # 設定任務順序
    create_table_task >> process_tasks