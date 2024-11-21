import requests
import pandas as pd
import pymysql
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from dotenv import load_dotenv

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="cinfo_join_industry_dag",
    default_args=default_args,
    description="update company_info_join_industry table",
    schedule_interval="30 0 * * 1-5",
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def cinfo_join_industry_dag():
    @task
    def update_cinfo_join_industry():
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
        truncate company_info_join_industry;
        """
        cursor.execute(sql_truncate)
        conn.commit()

        # insert
        sql_insert = """
        insert into company_info_join_industry
        select report_date, stock_code, company_abbreviation, industry_name, industry_code
        from company_info c
        join industry i
        on c.industry = i.industry_code;
        """

        cursor.execute(sql_insert)
        conn.commit()
        
        cursor.close()
        conn.close()

    # Task dependencies
    update_cinfo_join_industry()

cinfo_join_industry_dag()