from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.baseoperator import chain
import requests
import pandas as pd
from io import StringIO
import time
import random
import pymysql
import os
from dotenv import load_dotenv
import logging

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
        CREATE TABLE IF NOT EXISTS balance_sheet (
            stock_code VARCHAR(10),
            year VARCHAR(3),
            quarter VARCHAR(1),
            company_name VARCHAR(100),
            current_assets DECIMAL(20, 2),
            non_current_assets DECIMAL(20, 2),
            total_assets DECIMAL(20, 2),
            current_liabilities DECIMAL(20, 2),
            non_current_liabilities DECIMAL(20, 2),
            total_liabilities DECIMAL(20, 2),
            treasury_stock DECIMAL(20, 2),
            parent_equity DECIMAL(20, 2),
            joint_control_equity DECIMAL(20, 2),
            non_control_equity DECIMAL(20, 2),
            total_equity DECIMAL(20, 2),
            reference_net_value DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY `unique_id_year_quarter` (stock_code, year, quarter)
        )
        """
        cursor.execute(sql)
    conn.commit()
    conn.close()

def safe_decimal(value):
    if value == '--' or value == 'N/A' or pd.isna(value):
        return 0
    try:
        return float(value)
    except ValueError:
        return 0

@dag(
    dag_id="bs_scraper_dag",
    default_args=default_args,
    description="DAG for scraping balance sheet data",
    schedule_interval="0 0 * * 1-5",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["scraper", "balance_sheet"]
)
def bs_scraper_dag():
    @task
    def create_table():
        create_table_if_not_exists()
        logger.info("Table created or verified")

    @task
    def get_current_year_and_season():
        today = datetime.now()
        year = today.year - 1911  # 轉換為民國年
        # 根據月份決定季度
        month = today.month
        if month <= 3:
            season = 4
            year -= 1  # 如果是1-3月，要拿上一年第4季的資料
        elif month <= 6:
            season = 1
        elif month <= 9:
            season = 2
        else:
            season = 3
        logger.info(f"Current year: {year}, season: {season}")
        return year, season

    @task
    def get_bs(year_season):
        year, season = year_season
        TYPEK = 'sii'
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'
        parameter = {'firstin': '1', 'TYPEK': TYPEK, 'year': str(year), 'season': str(season)}
        max_retries = 3

        for attempt in range(max_retries):
            try:
                res = requests.post(url, data=parameter)
                res.raise_for_status()

                tables = pd.read_html(StringIO(res.text))
                logger.info(f"Found {len(tables)} tables")

                if len(tables) > 3:
                    df = tables[3]
                    logger.info(f"Columns found: {df.columns}")
                else:
                    logger.warning(f"Unexpected table structure for Year {year}, Season {season}. Skipping...")
                    return None

                # Instead of returning all data at once, just return year and season
                # The actual data will be processed directly in get_bs
                conn = connect_db()
                try:
                    with conn.cursor() as cursor:
                        sql = """
                        INSERT INTO balance_sheet (
                            stock_code, year, quarter, company_name, current_assets,
                            non_current_assets, total_assets, current_liabilities, non_current_liabilities,
                            total_liabilities, treasury_stock, parent_equity, joint_control_equity,
                            non_control_equity, total_equity, reference_net_value
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            company_name = VALUES(company_name),
                            current_assets = VALUES(current_assets),
                            non_current_assets = VALUES(non_current_assets),
                            total_assets = VALUES(total_assets),
                            current_liabilities = VALUES(current_liabilities),
                            non_current_liabilities = VALUES(non_current_liabilities),
                            total_liabilities = VALUES(total_liabilities),
                            treasury_stock = VALUES(treasury_stock),
                            parent_equity = VALUES(parent_equity),
                            joint_control_equity = VALUES(joint_control_equity),
                            non_control_equity = VALUES(non_control_equity),
                            total_equity = VALUES(total_equity),
                            reference_net_value = VALUES(reference_net_value)
                        """

                        for _, row in df.iterrows():
                            cursor.execute(sql, (
                                str(row.get('公司 代號', 'N/A')),
                                year,
                                season,
                                row.get('公司名稱', 'N/A'),
                                safe_decimal(row.get('流動資產', 'N/A')),
                                safe_decimal(row.get('非流動資產', 'N/A')),
                                safe_decimal(row.get('資產總計', 'N/A')),
                                safe_decimal(row.get('流動負債', 'N/A')),
                                safe_decimal(row.get('非流動負債', 'N/A')),
                                safe_decimal(row.get('負債總計', row.get('負債總額', 'N/A'))),
                                safe_decimal(row.get('庫藏股票', 'N/A')),
                                safe_decimal(row.get('歸屬於母公司業主之權益合計', 'N/A')),
                                safe_decimal(row.get('共同控制下前手權益', 'N/A')),
                                safe_decimal(row.get('非控制權益', 'N/A')),
                                safe_decimal(row.get('權益總計', row.get('權益總額', 'N/A'))),
                                safe_decimal(row.get('每股參考淨值', 'N/A'))
                            ))
                    conn.commit()
                    logger.info(f"Data processed for Year {year}, Season {season}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error processing data: {str(e)}")
                    raise
                finally:
                    conn.close()

                return {'year': year, 'season': season, 'status': 'success'}

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(1, 3))
                else:
                    logger.error(f"Unable to fetch data: Year {year}, Season {season}")
                    return None

    # Define tasks
    create_table_task = create_table()
    year_season_task = get_current_year_and_season()
    bs_data_task = get_bs(year_season_task)

    # Set up the task order using chain
    chain(create_table_task, year_season_task, bs_data_task)

# Instantiate the DAG
bs_scraper_dag_instance = bs_scraper_dag()