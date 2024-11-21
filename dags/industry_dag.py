import requests
import pymysql
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from bs4 import BeautifulSoup
from dotenv import load_dotenv

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="industry_dag",
    default_args=default_args,
    description="get industry info",
    schedule_interval="0 0 * * 1-5",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["industry"]
)
def industry_dag():
    @task
    def industry():
        # 取得資料
        url ='https://isin.twse.com.tw/isin/class_i.jsp?kind=2&owncode=&stockname=&isincode=&markettype=1&issuetype=&industry_code='
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        industry = soup.find('select', {'name' : 'industry_code'}).findAll('option')
        industry_code = []
        for i in range(1, len(industry)):
            industry_code.append((industry[i].text[0:2], industry[i].text[3:]))

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
        truncate table industry;
        """
        cursor.execute(sql_truncate)
        conn.commit()

        # insert
        sql_insert_many = """
        insert into industry
        values (%s, %s);
        """
        cursor.executemany(sql_insert_many, industry_code)
        conn.commit()

        # delete
        sql_delete = """
        delete from industry
        where industry_code = 17;
        """
        cursor.execute(sql_delete)
        conn.commit()

        cursor.close()
        conn.close()
    
    # Task dependencies
    industry()

industry_dag()