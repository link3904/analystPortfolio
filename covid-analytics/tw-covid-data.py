import requests
import json
import pandas as pd

url = 'https://covid-19.nchc.org.tw/api/covid19?CK=covid-19@nchc.org.tw&querydata=5002&limited=%E5%85%A8%E9%83%A8%E7%B8%A3%E5%B8%82'

res = requests.get(url)
result = json.loads(res.text)

df = pd.DataFrame(result,index=(range(len(result))))
df.set_index('id',inplace=True)
df.drop(columns=['a01'],inplace=True)
df['a02'] = pd.to_datetime(df['a02'])
df['year'] = df['a02'].dt.year
df['month'] = df['a02'].dt.month
df[(df['year']==2022) & (df['month']==4) & (df['a03']=='台北市') & (df['a04']=='全區')]

df[(df['year']==2022) & (df['month']==4) & (df['a03']=='新北市') & (df['a04']=='全區')]
