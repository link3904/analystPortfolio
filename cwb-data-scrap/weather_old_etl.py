from dateutil.relativedelta import relativedelta
from datetime import datetime, date, time
from bs4 import BeautifulSoup
from core import getGBQdata
import os
import sys
import requests
import json
import numpy as np
import pandas as pd
import time as ts
import urllib
import pandas_gbq as gbq
sys.path.append(os.environ[''])

# A few variables were removed due to security reasons
PROJECT_ID = os.environ['PROJECT_ID']
DATASET_ID = ''
STN_LIST = ''
DEST_TABLE_ID = ''

date_param = sys.argv[1]
start_date = datetime.strptime(date_param, "%Y-%m-%d").date()


def get_station_list():
    print('Pulling Station List...')
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_ID}.{STN_LIST}`
    """
    df_stn_list = getGBQdata(PROJECT_ID, query)
    df_stn_list = df_stn_list[(df_stn_list['city'] == '新北市') |
                              (df_stn_list['city'] == '臺北市')
                              | (df_stn_list['city'] == '臺中市')
                              | (df_stn_list['city'] == '桃園市')
                              | (df_stn_list['city'] == '臺南市')
                              | (df_stn_list['city'] == '高雄市')]
    return df_stn_list


def get_weather_data(df_stn_list):
    print('Getting Weather Data Daily...')
    df_weather = pd.DataFrame()
    start_date_str = start_date.strftime("%Y-%m-%d")
    for idx in df_stn_list.index.tolist():
        for attempt in range(10):
            try:
                stn_name = df_stn_list.loc[idx, 'station_name']
                stn_id = df_stn_list.loc[idx, 'station_id']
                stn_name_encode = urllib.parse.quote(stn_name)
                stn_name_encode = urllib.parse.quote(stn_name_encode)
                print(stn_name)
                url = f'https://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station={stn_id}&stname={stn_name_encode}&datepicker={start_date_str}'
                print(url)
                df_data = pd.read_html(url, header=1, flavor='lxml')[0]
            except:
                print('Attempt {}'.format(attempt + 1))
                ts.sleep(10)
                continue
            break
        cols = df_data.columns.tolist()
        for i in cols:
            df_data.rename(columns={i: df_data[i][0]}, inplace=True)
        df_data.drop(index=0, inplace=True)
        df_data.reset_index(inplace=True, drop=True)
        df_data['Date'] = start_date
        df_data['StnName'] = stn_name
        df_data['ObsTime'] = df_data['ObsTime'].apply(int)
        df_data['ObsTime'] = df_data['ObsTime'] - 1
        df_weather = df_weather.append(df_data)
    df_weather.reset_index(inplace=True, drop=True)
    df_weather['CreatedAt'] = df_weather.apply(
        lambda x: pd.datetime.combine(x['Date'], time(x['ObsTime'])), 1)
    df_weather = df_weather[['StnName', 'CreatedAt', 'StnPres', 'SeaPres', 'Temperature',
                             'Td dew point', 'RH', 'WS', 'WD', 'WSGust', 'WDGust', 'Precp',
                             'PrecpHour', 'SunShine', 'GloblRad', 'Visb', 'UVI', 'Cloud Amount']]
    return df_weather


def clean_weather_data(df_weather):
    # Cleaning All Columns
    for col in df_weather.columns:
        if col == 'CreatedAt':
            continue
        null_idx = df_weather.loc[df_weather[col].isin(
            ['...', '/', 'x', 'X'])].index.tolist()
        df_weather.at[null_idx, col] = np.nan

    # Cleaning only Wind Direction
    null_idx = df_weather.loc[df_weather['WD'].isin(['V', 'v'])].index.tolist()
    df_weather.at[null_idx, 'WD'] = np.nan

    # Cleaning only Precp Column
    null_idx = df_weather.loc[df_weather['Precp'].isin(
        ['T', 'T'])].index.tolist()
    df_weather.at[null_idx, 'Precp'] = 0

    col_map = {'StnName': 'stn_name',
               'CreatedAt': 'created_at',
               'StnPres': 'stn_pres',
               'SeaPres': 'sea_pres',
               'Temperature': 'temp',
               'Td dew point': 'dew_pt',
               'RH': 'humidity',
               'WS': 'wind_spd',
               'WD': 'wind_dir',
               'WSGust': 'wind_spd_max',
               'WDGust': 'wind_dir_max',
               'Precp': 'precp',
               'PrecpHour': 'precp_time',
               'SunShine': 'sun_time',
               'GloblRad': 'globl_rad',
               'Visb': 'visb',
               'UVI': 'uvi',
               'Cloud Amount': 'cloud_index'}
    df_weather.rename(columns=col_map, inplace=True)
    return df_weather


def upload_to_bq(df_weather):
    rows = len(df_weather)
    print(f'Inserting rows:{rows}')
    print(f'--Inserting data for {start_date}--')
    msg = gbq.to_gbq(df_weather, f'{DATASET_ID}.{DEST_TABLE_ID}',
                     PROJECT_ID, if_exists='append')
    assert msg == None
    print('Upload Successful.')
    return


def main():
    df_stn_list = get_station_list()
    df_weather = get_weather_data(df_stn_list)
    df_weather = clean_weather_data(df_weather)
    upload_to_bq(df_weather)
    return


if __name__ == "__main__":
    main()
