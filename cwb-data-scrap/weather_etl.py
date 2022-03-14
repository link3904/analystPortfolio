import os
import sys
import time
import urllib
import requests
import pandas_gbq as gbq
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime

# A few variables were removed due to security reasons
sys.path.append(os.environ[""])

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET_ID = ""
STN_LS_TABLE_ID = ""
WEATHER_LIVE_TABLE_ID = ""
CWB_TOKEN = os.environ["CWB_TOKEN"]
WEATHER_API = "O-A0001-001"
PRECIP_API = "O-A0002-001"


def update_station_list():
    print("Fetching Station List...")
    station_list_url = "https://e-service.cwb.gov.tw/wdps/obs/state.htm"
    resp = requests.get(station_list_url, verify=False)
    resp.encoding = "Big5"
    soup = BeautifulSoup(resp.text)
    table = soup.find("table", {"class": "MsoNormalTable"})
    table_rows = table.find_all("tr")
    l = []
    for tr in table_rows:
        td = tr.find_all("td")
        row = [tr.text.strip() for tr in td]
        l.append(row)
    df = pd.DataFrame(l, columns=[i for i in range(13)])
    print("Cleaning Raw Data...")
    columns = df.iloc[1, :].tolist()
    df = df.iloc[2:, :]
    df.columns = columns
    stop_idx = df[df["站號"].fillna("0").str.contains("已撤銷")].index[0]
    df = df.loc[: stop_idx - 3, :]
    df = df.iloc[:, :-5]
    df.columns = [
        "station_id",
        "station_name",
        "sea_level_m",
        "lng",
        "lat",
        "city",
        "address",
        "created_at",
    ]
    df.sea_level_m = df.sea_level_m.astype(float)
    df.lng = df.lng.astype(float)
    df.lat = df.lat.astype(float)
    df.created_at = df.created_at.apply(
        lambda x: datetime.strptime(x, "%Y/%m/%d"))
    print("Uploading to GBQ...")
    msg = gbq.to_gbq(
        df, f"{DATASET_ID}.{STN_LS_TABLE_ID}", PROJECT_ID, if_exists="replace"
    )
    assert msg == None, print("Upload Failed.")
    print("Upload Successful.")
    return df


def parse_result(result):
    data = {}
    v_list = []
    df_weather_data = pd.DataFrame()
    for k, v in result.items():
        if type(v) == type([1, 2, 3]):
            for e in v:
                for k2, v2 in e.items():
                    v_list.append(v2)
                data[v_list[0]] = v_list[1]
                v_list = []
        elif type(v) == type("foo"):
            data[k] = v
        elif type(v) == type({"key": "value"}):
            for k3, v3 in v.items():
                data[k3] = v3
    df_weather_data = df_weather_data.append(pd.DataFrame(data=[data]))
    return df_weather_data


def pull_weather_data(df_stn_key):
    print("Scraping weather data...")
    stn_list = df_stn_key[
        (df_stn_key["city"] == "新北市")
        | (df_stn_key["city"] == "臺北市")
        | (df_stn_key["city"] == "臺中市")
        | (df_stn_key["city"] == "桃園市")
        | (df_stn_key["city"] == "臺南市")
        | (df_stn_key["city"] == "高雄市")
    ]
    stn_list = stn_list["station_name"].values.tolist()
    df_weather = pd.DataFrame()
    df_precip = pd.DataFrame()
    for y in stn_list:
        loc_name = urllib.parse.quote(y)
        for attempt in range(10):
            try:
                print(y)
                rain_url = f"https://opendata.cwb.gov.tw/api/v1/rest/datastore/{PRECIP_API}?Authorization={CWB_TOKEN}&format=JSON&locationName={loc_name}"
                response = requests.get(rain_url)
                break
            except:
                print("Attempt {}".format(attempt + 1))
                time.sleep(10)
                continue
        jsonResponse = response.json()
        if jsonResponse["records"]["location"] != []:
            result = jsonResponse["records"]["location"][0]
        else:
            print("Empty Observation.")
            continue
        precip_result = parse_result(result)
        df_precip = df_precip.append(precip_result)
        for attempt in range(10):
            try:
                url = f"https://opendata.cwb.gov.tw/api/v1/rest/datastore/{WEATHER_API}?Authorization={CWB_TOKEN}&format=JSON&locationName={loc_name}"
                response = requests.get(url)
                break
            except:
                print("Attempt {}".format(attempt + 1))
                time.sleep(10)
                continue
        jsonResponse = response.json()
        if jsonResponse["records"]["location"] != []:
            result = jsonResponse["records"]["location"][0]
        else:
            print("Empty Observation.")
            continue
        weather_result = parse_result(result)
        df_weather = df_weather.append(weather_result)

    df = pd.merge(
        left=df_weather,
        right=df_precip,
        on=[
            "lat",
            "lon",
            "locationName",
            "stationId",
            "ELEV",
            "CITY",
            "CITY_SN",
            "TOWN",
            "TOWN_SN",
        ],
        how="left",
    )
    return df


def clean_weather_data(df):
    print("Cleaning weather data...")
    print("Renaming Columns...")
    df.drop(
        columns=[
            "MIN_10",
            "HOUR_3",
            "HOUR_6",
            "HOUR_12",
            "HOUR_24",
            "NOW",
            "latest_2days",
            "latest_3days",
            "obsTime_y",
            "ATTRIBUTE",
            "H_24R",
        ],
        inplace=True,
    )
    df.rename(
        columns={
            "lon": "lng",
            "locationName": "station_name",
            "stationId": "station_id",
            "obsTime_x": "obs_dt",
            "ELEV": "elevation",
            "WDIR": "wind_dir",
            "WDSD": "wind_spd",
            "TEMP": "temp",
            "HUMD": "humidity",
            "PRES": "pressure",
            "RAIN": "cum_precp_hour",
            "H_FX": "max_wind_spd",
            "H_XD": "max_wind_dir",
            "H_FXT": "max_wind_ts",
            "D_TX": "max_temp",
            "D_TXT": "max_temp_ts",
            "D_TN": "min_temp",
            "D_TNT": "min_temp_ts",
            "CITY": "city",
            "CITY_SN": "city_id",
            "TOWN": "town",
            "TOWN_SN": "town_id",
        },
        inplace=True,
    )

    print("Converting data types...")
    df_na = df[(pd.isna(df["obs_dt"])) | (df["obs_dt"] == "")]
    df_not = df[~(pd.isna(df["obs_dt"])) & ~(df["obs_dt"] == "-99")]
    df_not["obs_dt"] = df_not["obs_dt"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
    )
    df = pd.concat([df_na, df_not], axis=0)

    df_na = df[(pd.isna(df["max_temp_ts"])) | (df["max_temp_ts"] == "-99")]
    df_not = df[~(pd.isna(df["max_temp_ts"])) & ~(df["max_temp_ts"] == "-99")]
    df_not["max_temp_ts"] = df_not["max_temp_ts"].apply(
        lambda x: datetime.strptime(x[:-6], "%Y-%m-%dT%H:%M:%S")
    )
    df_not["max_temp_ts"] = pd.to_datetime(df_not["max_temp_ts"])
    df_na["max_temp_ts"] = np.datetime64("NaT")
    df = pd.concat([df_na, df_not], axis=0)

    df_na = df[(pd.isna(df["min_temp_ts"])) | (df["min_temp_ts"] == "-99")]
    df_not = df[~(pd.isna(df["min_temp_ts"])) & ~(df["min_temp_ts"] == "-99")]
    df_not["min_temp_ts"] = df_not["min_temp_ts"].apply(
        lambda x: datetime.strptime(x[:-6], "%Y-%m-%dT%H:%M:%S")
    )
    df_not["min_temp_ts"] = pd.to_datetime(df_not["min_temp_ts"])
    df_na["min_temp_ts"] = np.datetime64("NaT")
    df = pd.concat([df_na, df_not], axis=0)

    df_na = df[pd.isna(df["max_wind_ts"]) | (df["max_wind_ts"] == "-99")]
    df_not = df[~(pd.isna(df["max_wind_ts"])) & ~(df["max_wind_ts"] == "-99")]
    df_not["max_wind_ts"] = df_not["max_wind_ts"].apply(
        lambda x: datetime.strptime(x[:-6], "%Y-%m-%dT%H:%M:%S")
    )
    df_not["max_wind_ts"] = pd.to_datetime(df_not["max_wind_ts"])
    df_na["max_wind_ts"] = np.datetime64("NaT")
    df = pd.concat([df_na, df_not], axis=0)

    df["lat"] = df["lat"].apply(eval)
    df["lng"] = df["lng"].apply(eval)
    df["elevation"] = df["elevation"].apply(eval)
    df["wind_dir"] = df["wind_dir"].apply(eval)
    df["wind_dir"] = df["wind_dir"].apply(lambda x: np.nan if x == -99 else x)
    df["wind_spd"] = df["wind_spd"].apply(eval)
    df["temp"] = df["temp"].apply(eval)
    df["humidity"] = df["humidity"].apply(eval)
    df["pressure"] = df["pressure"].apply(eval)
    df["pressure"] = df["pressure"].apply(lambda x: np.nan if x == -99 else x)
    df["cum_precp_hour"] = df["cum_precp_hour"].fillna("0")
    df["cum_precp_hour"] = df["cum_precp_hour"].apply(eval)
    df["max_wind_spd"] = df["max_wind_spd"].apply(eval)
    df["max_wind_spd"] = df["max_wind_spd"].apply(
        lambda x: np.nan if x == -99 else x)
    df["max_wind_dir"] = df["max_wind_dir"].apply(eval)
    df["max_wind_dir"] = df["max_wind_dir"].apply(
        lambda x: np.nan if x == -99 else x)
    df["max_temp"] = df["max_temp"].apply(eval)
    df["min_temp"] = df["min_temp"].apply(eval)

    df = df[
        [
            "station_id",
            "station_name",
            "obs_dt",
            "lat",
            "lng",
            "elevation",
            "wind_dir",
            "wind_spd",
            "temp",
            "humidity",
            "pressure",
            "cum_precp_hour",
            "max_wind_spd",
            "max_wind_dir",
            "max_wind_ts",
            "max_temp",
            "max_temp_ts",
            "min_temp",
            "min_temp_ts",
            "city",
            "city_id",
            "town",
            "town_id",
        ]
    ]
    return df


def insert_weather_data(df_insert):
    msg = gbq.to_gbq(
        df_insert,
        f"{DATASET_ID}.{WEATHER_LIVE_TABLE_ID}",
        PROJECT_ID,
        if_exists="append",
    )
    assert msg == None, print("Upload Failed.")
    print("Upload Successful.")
    return


def main():
    df_stn_key = update_station_list()
    df_weather_data = pull_weather_data(df_stn_key)
    df = clean_weather_data(df_weather_data)
    insert_weather_data(df)
    return


if __name__ == "__main__":
    main()
