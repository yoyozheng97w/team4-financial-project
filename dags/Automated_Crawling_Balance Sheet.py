import requests
import pandas as pd
import time
import random  # 引入随机模块
from datetime import datetime
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_balance_sheet(TYPEK, year, season, retries=3):
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'
    parameter = {'firstin': '1', 'TYPEK': TYPEK,
                 'year': str(year), 'season': str(season)}

    for attempt in range(retries):
        try:
            res = requests.post(url, data=parameter)
            res.raise_for_status()  # 检查请求是否成功
            tables = pd.read_html(res.text)

            if len(tables) < 4:
                raise Exception("未找到足夠的表格")

            df = tables[3]  # 假设第4个表格是正确的
            df.insert(1, '年度', year)
            df.insert(2, '季別', season)
            print(f"抓取到的資料形狀: {df.shape}")
            return df
        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            if attempt < retries - 1:
                wait_time = 2 ** attempt  # 指数退避
                print(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                raise Exception(f"无法抓取 {year} 年第 {season} 季的资料，已重试 {retries} 次")

def auto_fetch_data(TYPEK, start_year, start_season, **kwargs):
    current_year = datetime.now().year - 1911
    current_month = datetime.now().month
    current_day = datetime.now().day

    # 判斷當前季別
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

    # 迴圈以抓取資料
    while (year < current_year) or (year == current_year and season <= current_season):
        print(f'Fetching data for year {year} season {season}...')

        try:
            df = get_balance_sheet(TYPEK, year, season)  # 確保這裡有正確的資料抓取邏輯
            all_data = pd.concat([all_data, df], ignore_index=True)
        except Exception as e:
            print(f'Unable to fetch data for year {year} season {season}: {e}')
        
        sleep_time = random.randint(3, 5)  # 生成随机的睡眠时间，范围在5到8秒
        print(f'等待 {sleep_time} 秒后再进行下一次请求...')
        time.sleep(sleep_time)
        
        # 控制季節變換
        if season < 4:
            season += 1
        else:
            season = 1
            year += 1

    # 儲存抓取的資料到 MySQL
    engine = create_engine('mysql+pymysql://airflow:airflow@mysql:3306/financial')
    try:
        all_data.to_sql('balance_sheet', con=engine, if_exists='replace', index=False)
        print("資料匯入成功！")
    except Exception as e:
        print(f"發生錯誤：{e}")

    # 檢查匯入後的資料量
    with engine.connect() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM balance_sheet"))
        count = result.scalar()
        print(f"目前在 balance_sheet 表中的資料量為: {count} 筆")

default_args = {
    'start_date': datetime(2023, 10, 21),
    'retries': 1,
}

with DAG('fetch_financial_data2_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_financial_data2',
        python_callable=auto_fetch_data,
        op_kwargs={'TYPEK': 'sii', 'start_year': 103, 'start_season': 1}
    )