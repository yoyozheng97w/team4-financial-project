from airflow import DAG
from airflow.decorators import task
import pandas as pd
from datetime import datetime, timedelta
import pymysql
import os
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()

# 數據庫連線函數
def connect_db():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT')),
        user=os.getenv('DB_USER'),
        passwd=os.getenv('DB_PASSWORD'),
        charset=os.getenv('DB_CHARSET'),
        db=os.getenv('DB_NAME')
    )

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

# 創建 DAG 實例
with DAG(
    dag_id="dividend_yield_yearly_average",
    default_args=default_args,
    description="Calculate yearly average dividend yield for stocks",
    schedule_interval="45 19 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["calculator", "dividend_yield"]
) as dag:
    
    @task
    def process_dividend_yield():
        """處理股息收益率數據：提取、轉換並加載到新表"""
        try:
            # 1. 提取數據
            conn = connect_db()
            query = """
            SELECT 
                ex_dividend_date,
                stock_code,
                dividend_yield
            FROM dividend_yield
            """
            df = pd.read_sql(query, conn)
            conn.close()

            # 2. 轉換數據
            # 添加年份列
            df['format_year'] = pd.to_datetime(df['ex_dividend_date']).dt.year
            
            # 計算年度平均股息率
            yearly_avg = df.groupby(['format_year', 'stock_code'])['dividend_yield'].mean().reset_index()
            
            # 找出2020-2024年都有數據的股票
            stock_years = yearly_avg[yearly_avg['format_year'].between(2020, 2024)].groupby('stock_code').size()
            complete_stocks = stock_years[stock_years == 5].index
            
            # 篩選最終結果
            final_df = yearly_avg[
                (yearly_avg['format_year'].between(2020, 2024)) & 
                (yearly_avg['stock_code'].isin(complete_stocks))
            ]

            # 3. 加載數據
            conn = connect_db()
            cursor = conn.cursor()
            
            # 創建新表（如果不存在）- 修改stock_code為VARCHAR並指定長度
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS dividend_yield_yr_avg (
                format_year DATETIME,
                stock_code VARCHAR(20),
                avg_dividend_yield DOUBLE,
                PRIMARY KEY (format_year, stock_code)
            )
            """
            cursor.execute(create_table_sql)
            
            # 準備插入數據
            insert_sql = """
            INSERT INTO dividend_yield_yr_avg 
            (format_year, stock_code, avg_dividend_yield)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            avg_dividend_yield = VALUES(avg_dividend_yield)
            """
            
            # 將DataFrame轉換為待插入的數據列表
            values = final_df.apply(
                lambda row: (
                    datetime(row['format_year'], 1, 1),  # 將年份轉換為datetime格式
                    row['stock_code'],
                    row['dividend_yield']
                ),
                axis=1
            ).tolist()
            
            # 執行批量插入
            cursor.executemany(insert_sql, values)
            conn.commit()
            
        except Exception as e:
            if 'conn' in locals() and conn:
                conn.rollback()
            raise Exception(f"處理失敗: {str(e)}")
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()

    # 執行任務
    process_dividend_yield()