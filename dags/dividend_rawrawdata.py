import requests
from bs4 import BeautifulSoup
import csv
import re
import time
import random

# 定義公司代碼抓取網址
company_list_url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
# 設定 User-Agent
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"
}

# 發送 GET 請求並抓取公司列表
response = requests.get(company_list_url, headers=headers)
response.encoding = 'big5'  # 設定編碼以正確解析中文

# 檢查請求是否成功
if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'h4'})
    company_list = []

    # 遍歷表格的每一行，找出長度為4的公司代碼
    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        if len(cols) > 1:
            code_name = cols[0].get_text(strip=True)
            if '　' in code_name:
                code, name = code_name.split('　', 1)
                if len(code) == 4 and code.isdigit():  # 過濾出長度為4的公司代碼
                    company_list.append(code)

    # 遍歷每個公司代碼，代入 A 程式碼進行股利資料的爬蟲
    for stock_code in company_list:
        # 隨機等待1到5秒
        time.sleep(random.randint(1, 5))

        # A程式碼部分開始
        url = f"https://tw.stock.yahoo.com/quote/{stock_code}.TWO/dividend"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            table_body = soup.find("div", class_="table-body")
            if table_body:
                rows = table_body.find_all("li", class_="List(n)")
                data_rows = []

                for row in rows:
                    columns = row.find_all("div")
                    data_row = [col.get_text(strip=True) for col in columns]
                    
                    # 濾除出我們需要的欄位
                    parsed_data = [stock_code]  # 加入股票代碼
                    
                    # 抓取各項股利資料
                    parsed_data.append(data_row[1])  # 發放年份
                    parsed_data.append(data_row[2])  # 前一年
                    parsed_data.append(data_row[4])  # 股利值
                    parsed_data.append(data_row[5])  # 固定符號
                    parsed_data.append(data_row[6])  # 百分比
                    parsed_data.append(data_row[7])  # 股價
                    parsed_data.append(data_row[8])  # 除息日期
                    parsed_data.append(data_row[9])  # 固定符號
                    parsed_data.append(data_row[10]) # 除息結束日
                    parsed_data.append(data_row[11]) # 固定符號
                    parsed_data.append(data_row[12]) # 天數

                    data_rows.append(parsed_data)
                
                # 將每支股票的股利資料附加到 CSV
                with open('stock_dividend_data.csv', 'a', newline='', encoding='utf-8-sig') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerows(data_rows)
                
                print(f"{stock_code} 的股利資料已成功寫入 'stock_dividend_data.csv'")
            else:
                print(f"{stock_code} 無法找到包含股利資料的區域。")
        else:
            print(f"無法取得 {stock_code} 的網頁，狀態碼：", response.status_code)
else:
    print(f"無法訪問公司列表網頁，狀態碼：{response.status_code}")
