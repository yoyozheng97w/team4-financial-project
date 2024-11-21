import requests
import pandas as pd
import pymysql
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from dotenv import load_dotenv

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="company_info_one_task",
    default_args=default_args,
    description="get company basic info from api",
    schedule_interval="0 0 * * 1-5",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["company_info"]
)
def company_info_one_task():
    @task
    def company_info_api():
        # 取得資料
        url = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
        res = requests.get(url)  
        df = pd.DataFrame(res.json())

        # transfer date
        df['出表日期'] = df['出表日期'].apply(lambda x: str(int(x[0:3]) + 1911) + x[3:])

        # transfer "－" to null
        df['外國企業註冊地國'] = df['外國企業註冊地國'].apply(lambda x: None if x.strip() == '－' else x)

        info = df.loc[:, ['出表日期', '公司代號', '公司名稱', '公司簡稱', 
                          '外國企業註冊地國', '產業別', '營利事業統一編號', 
                          '成立日期', '上市日期']]

        # update_company_info_table():
        load_dotenv()
        config = {
            "host": os.getenv('DB_HOST'),
            "port": int(os.getenv('DB_PORT')),
            "user": os.getenv('DB_USER'),
            "password": os.getenv('DB_PASSWORD'),
            "db": os.getenv('DB_NAME'),
            "charset":  os.getenv('DB_CHARSET'),
        }

        conn = pymysql.connect(**config)
        cursor = conn.cursor()

        # truncate
        sql_truncate = """
        truncate table company_info;
        """
        cursor.execute(sql_truncate)
        conn.commit()

        # insert
        sql_insert_many = """
        insert into company_info
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        values = list(info.itertuples(index=False, name=None))

        cursor.executemany(sql_insert_many, values)
        conn.commit()
        
        # delete
        sql_delete = """
        delete from company_info
        where stock_code in (1409,1718,2207,2905);
        """

        cursor.execute(sql_delete)
        conn.commit()
        
        cursor.close()
        conn.close()
    
    # Task dependencies
    company_info_api()

company_info_one_task()