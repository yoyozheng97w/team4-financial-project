import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Load environment variables from .env file
load_dotenv()

mysql_ip = os.getenv('DB_HOST')
mysql_port = os.getenv('DB_PORT')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_NAME')
charset = os.getenv('DB_CHARSET', 'utf8mb4')

source_table_highest = "stock_prices_rawdata"
source_table_dividend = "dividend_yield"
target_table_fill_dividend = "fill_dividend"

connection_string = f"mysql+pymysql://{user}:{password}@{mysql_ip}:{mysql_port}/{database}?charset={charset}"

def create_mysql_engine():
    return create_engine(connection_string, future=True)

def trans_highest(engine):
    query = f"""
        SELECT record_date, stock_code, highest_price
        FROM {source_table_highest}"""
    with engine.connect() as connection:
        data = pd.read_sql(text(query), con=connection)
    return data.pivot(index='record_date', columns='stock_code', values='highest_price').reset_index()

def convert_to_gregorian(minguo_date):
    try:
        year_str, rest = minguo_date.split("\u5e74", 1)
        return f"{int(year_str) + 1911}\u5e74{rest}"
    except:
        return minguo_date

def calculate_fill_dividend_with_epsilon_fixed(test_row, close_prices_df, test_df):
    stock_code = str(test_row['stock_code'])
    if stock_code not in close_prices_df.columns:
        return None, None
    
    ex_right_date = test_row['ex_dividend_date']
    ex_right_price = pd.to_numeric(test_row['pre_ex_dividend_close_price'], errors='coerce')
    epsilon = 0.01

    next_ex_right_record = test_df[(test_df['stock_code'] == stock_code) & (test_df['ex_dividend_date'] > ex_right_date)]
    next_ex_right_date = next_ex_right_record['ex_dividend_date'].min() if not next_ex_right_record.empty else close_prices_df['record_date'].max()

    first_day_row = close_prices_df[close_prices_df['record_date'] == ex_right_date]
    if not first_day_row.empty:
        closing_price_first_day = pd.to_numeric(first_day_row[stock_code].values[0], errors='coerce')
        if closing_price_first_day >= ex_right_price - epsilon:
            return True, 1

    relevant_rows = close_prices_df[(close_prices_df['record_date'] > ex_right_date) &
                                    (close_prices_df['record_date'] < next_ex_right_date)].sort_values(by='record_date')
    
    for trading_day_count, (_, row) in enumerate(relevant_rows.iterrows(), start=2):
        closing_price = pd.to_numeric(row[stock_code], errors='coerce')
        if not pd.isna(closing_price) and closing_price >= ex_right_price - epsilon:
            return True, trading_day_count

    return False, None

def fill_dividend_calculation():
    engine = create_mysql_engine()
    close_prices_df = trans_highest(engine)
    close_prices_df["record_date"] = pd.to_datetime(close_prices_df["record_date"], format='%Y-%m-%d')
    
    with engine.connect() as connection:
        test_df = pd.read_sql(text(f"SELECT ex_dividend_date, stock_code, pre_ex_dividend_close_price FROM {source_table_dividend}"), con=connection)
    test_df["ex_dividend_date"] = pd.to_datetime(test_df["ex_dividend_date"].apply(convert_to_gregorian), format='%Y\u5e74%m\u6708%d\u65e5')

    results = test_df.apply(lambda row: calculate_fill_dividend_with_epsilon_fixed(row, close_prices_df, test_df), axis=1)
    test_df[['fill_result', 'trading_days']] = pd.DataFrame(results.tolist(), index=test_df.index)

    test_df_sorted = test_df[['stock_code', 'ex_dividend_date', 'fill_result', 'trading_days']].sort_values(by=['stock_code', 'ex_dividend_date'])

    # 檢查 DataFrame 是否有資料
    print("Data to be inserted:")
    print(test_df_sorted)

    with engine.begin() as conn:
        # 清空目標表格
        conn.execute(text("TRUNCATE TABLE fill_dividend"))
        print("Successfully truncated table: fill_dividend")
        
        # 插入資料並提交變更
        test_df_sorted.to_sql(target_table_fill_dividend, con=conn, if_exists='append', index=False)
        print("Successfully inserted records into table: fill_dividend")

def run_fill_dividend_calculation():
    fill_dividend_calculation()

local_tz = pendulum.timezone("Asia/Taipei")

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now(tz=local_tz).subtract(days=1),
}

dag = DAG(
    'fill_dividend_calculation_dag',
    default_args=default_args,
    description='A DAG to calculate and store fill dividend data',
    schedule_interval='0 20 * * 1-5',  # 每天晚上 8 點執行
    catchup=False,
    tags=['financial'],
)

fill_dividend_task = PythonOperator(
    task_id='fill_dividend_task',
    python_callable=run_fill_dividend_calculation,
    dag=dag,
)
