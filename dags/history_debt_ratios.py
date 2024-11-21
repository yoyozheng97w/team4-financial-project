from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.baseoperator import chain
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql
import logging
import os
from dotenv import load_dotenv

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def connect_db():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT')),
        user=os.getenv('DB_USER'),
        passwd=os.getenv('DB_PASSWORD'),
        charset=os.getenv('DB_CHARSET'),
        db=os.getenv('DB_NAME')
    )

def create_table_if_not_exists():
    conn = connect_db()
    with conn.cursor() as cursor:
        sql = """
        CREATE TABLE IF NOT EXISTS debt_ratios (
            stock_code VARCHAR(10),
            year VARCHAR(3),
            quarter VARCHAR(1),
            debt_to_assets_ratio DECIMAL(20, 2),
            current_ratio DECIMAL(20, 2),
            PRIMARY KEY (stock_code, year, quarter)
        )
        """
        cursor.execute(sql)
    conn.commit()
    conn.close()

@dag(
    dag_id="bs_history_debt_ratios_dag",
    default_args=default_args,
    description="DAG for calculating debt ratios data",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["calculator", "debt_ratios"]
)
def debt_ratios_dag():
    
    @task
    def create_table():
        create_table_if_not_exists()
        logger.info("Debt ratios table created or verified")
        
    @task
    def process_debt_ratios():
        """獲取資產負債表數據，計算並儲存債務比率"""
        try:
            # 從資料庫獲取數據
            conn = connect_db()
            query = """
            SELECT stock_code, year, quarter, company_name,
                   current_assets, non_current_assets, total_assets,
                   current_liabilities, non_current_liabilities, total_liabilities,
                   treasury_stock, parent_equity, joint_control_equity,
                   non_control_equity, total_equity, reference_net_value
            FROM balance_sheet
            WHERE (year < 113) OR (year = 113 AND quarter <= 2)
            ORDER BY year DESC, quarter DESC
            """
            df = pd.read_sql(query, conn)
            logger.info(f"Retrieved {len(df)} balance sheet records")
            
            # 計算債務比率
            debt_ratios = pd.DataFrame()
            debt_ratios['stock_code'] = df['stock_code']
            debt_ratios['year'] = df['year']
            debt_ratios['quarter'] = df['quarter']
            
            # 計算各項比率
            # debt_ratios['debt_ratio'] = (df['total_liabilities'] / df['total_assets'] * 100).round(2) 
            # debt_ratios['long_term_debt_to_equity'] = (df['non_current_liabilities'] / df['total_equity'] * 100).round(2)
            debt_ratios['debt_to_assets_ratio'] = (df['total_liabilities'] / df['total_assets'] * 100).round(2) #財務結構_負債佔資產比率
            # debt_ratios['equity_multiplier'] = (df['total_assets'] / df['total_equity']).round(2)
            # debt_ratios['current_liabilities_to_equity'] = (df['current_liabilities'] / df['total_equity'] * 100).round(2)
            # debt_ratios['non_current_liabilities_to_equity'] = (df['non_current_liabilities'] / df['total_equity'] * 100).round(2)
            # debt_ratios['total_debt_to_equity'] = (df['total_liabilities'] / df['total_equity'] * 100).round(2)
            debt_ratios['current_ratio'] = (df['current_assets'] / df['current_liabilities'] * 100).round(2) #償債比率_流動比率

            # 處理異常值
            debt_ratios = debt_ratios.replace([np.inf, -np.inf], None)
            debt_ratios = debt_ratios.fillna(0)
            
            logger.info(f"Calculated debt ratios for {len(debt_ratios)} records")
            
            # 儲存數據
            with conn.cursor() as cursor:
                # 刪除現有數據
                delete_query = """
                DELETE FROM debt_ratios 
                WHERE (year < 113) OR (year = 113 AND quarter <= 2)
                """
                cursor.execute(delete_query)
                
                # 插入新數據
                insert_query = """
                INSERT INTO debt_ratios (
                    stock_code, year, quarter,
                    debt_to_assets_ratio, current_ratio
                ) VALUES (%s, %s, %s, %s, %s)
                """
                
                for index, row in debt_ratios.iterrows():
                    cursor.execute(insert_query, (
                        row['stock_code'], row['year'], row['quarter'],
                        row['debt_to_assets_ratio'], row['current_ratio'],
                    ))
                
            conn.commit()
            logger.info(f"Successfully saved {len(debt_ratios)} debt ratio records")
            
        except Exception as e:
            logger.error(f"Error in process_debt_ratios: {str(e)}")
            if 'conn' in locals():
                conn.rollback()
            raise
        finally:
            if 'conn' in locals():
                conn.close()

    # Define tasks
    create_table_task = create_table()
    process_task = process_debt_ratios()

    # Set up the task order using chain
    chain(create_table_task, process_task)

# Instantiate the DAG
debt_ratios_dag_instance = debt_ratios_dag()