import pandas as pd
import requests
import datetime
import aiohttp
import asyncio
from io import StringIO

today = datetime.date.today()
to_dt = today.replace(day=1) - datetime.timedelta(days=1)
from_dt = to_dt.replace(day=1).strftime('%Y-%m-%d')
to_dt = to_dt.strftime('%Y-%m-%d')

apps_data = pd.read_csv("https://docs.google.com/spreadsheets/d/1yDISkSGVhMy3EENx0z8EtcDtoxJmdDquJ6D8k82ZgfA/export?format=csv")
tokens = pd.read_excel(r'C:\Users\mixai\Desktop\API.xlsx').token.to_list()

lst_of_headers = [{"accept": "text/csv", "authorization": f"Bearer {api_token}"} for api_token in tokens]


lst_of_raw_url = []
base_url = 'https://hq1.appsflyer.com/api/raw-data/export/app/'
for apps_n_events in apps_data.itertuples():
    app = apps_n_events[1]
    event = apps_n_events[2]

    url = f'{base_url}{app}/in_app_events_report/v5?event_name={event}&from={from_dt}&to={to_dt}&maximum_rows=1000000'
    lst_of_raw_url.append(url)

lst_of_fraud_url = []
for apps_n_events in apps_data.itertuples():
    app = apps_n_events[1]
    event = apps_n_events[2]

    url = f'{base_url}{app}/fraud-post-inapps/v5?event_name={event}&from={from_dt}&to={to_dt}&maximum_rows=1000000'
    lst_of_fraud_url.append(url)

async def fetch(url, session, headers):
    async with session.get(url, headers=headers) as response:
        rep = await response.text()
        df = pd.read_csv(StringIO(rep), sep=',', on_bad_lines='skip')      
        if not df.empty and '<!DOCTYPE html>' not in df.columns:
            return df

async def fetch_all(urls, headers):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(url, session, headers) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    
raw_dataframes = [asyncio.run(fetch_all(lst_of_raw_url, i)) for i in lst_of_headers]

raw_lst = []
for i in range(len(raw_dataframes)):
    for df in raw_dataframes[i]:
        if type(df) != type(None):
            raw_lst.append(df)
raw_data = pd.concat(raw_lst).dropna(how='all')
raw_data.to_csv(r'G:\.shortcut-targets-by-id\1-0y0YvUdxNOM5ZnhM4sE0iJVter1OKIh\тест\ИнАпп\sql\Not_AF_export\Adjust\gdriveapikey\raw_export.csv')


fraud_dataframes = [asyncio.run(fetch_all(lst_of_fraud_url, i)) for i in lst_of_headers]

fraud_lst = []
for i in range(len(fraud_dataframes)):
    for df in fraud_dataframes[i]:
        if type(df) != type(None):
            fraud_lst.append(df)
fraud_data = pd.concat(fraud_lst).dropna(how='all')
fraud_data.to_csv(r'G:\.shortcut-targets-by-id\1-0y0YvUdxNOM5ZnhM4sE0iJVter1OKIh\тест\ИнАпп\sql\Not_AF_export\Adjust\gdriveapikey\fraud_export.csv')
# raw_data = pd.concat([*raw_dataframes[0], *raw_dataframes[1]])  

# fraud_dataframes = [asyncio.run(fetch_all(lst_of_fraud_url, i)) for i in lst_of_headers]

# fraud_data = pd.concat([*fraud_dataframes[0], *fraud_dataframes[1]])  