import requests
import os
import json
import pandas as pd
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

output_path = "/home/analytics/lavanya/diksha_infra/enrol/"

todays_date = date.today()
tomm_date = date.today() - timedelta(days=1)
start_date = tomm_date.strftime("%Y-%m-%d")
end_date = todays_date.strftime("%Y-%m-%d")


os.environ['TZ'] = 'UTC'


def Unique_Devices():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }

    query_str = {
  "queryType": "timeseries",
  "dataSource": "summary-distinct-counts",
  "aggregations": [
    {
      "type": "HLLSketchMerge",
      "name": "unique_devices",
      "fieldName": "unique_devices",
      "lgK": "12",
      "tgtHllType": "HLL_4"
    }
  ],
  "granularity": "all",
  "postAggregations": [],
  "intervals": start_date+"T00:00:00+00:00/"+end_date+"T00:00:00+00:00",
  "filter": {
    "type": "or",
    "fields": [
      {
        "type": "selector",
        "dimension": "dimensions_pdata_id",
        "value": "prod.diksha.app"
      },
      {
        "type": "selector",
        "dimension": "dimensions_pdata_id",
        "value": "prod.diksha.portal"
      }
    ]
  }
}

    jsondata = json.dumps(query_str)
    print(jsondata)
    final_list = []
    try:
        response = requests.post('http://11.4.0.53:8082/druid/v2', headers=headers, data=jsondata)

        x = response.json()
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x)
            df_pandas['Unique_Devices'] = df_pandas['result'].apply(lambda x:round(x['unique_devices']))
            df_pandas['Date'] = df_pandas['timestamp'].str.replace("T00:00:00.000Z"," ")
            df_slice          = df_pandas[['Date','Unique_Devices']]
            return df_slice
    except Exception as e:

        print(e)

def User_Sigin_Type():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI4ZjAzZWQwYzEyZTc0M2QyYThkODUwY2NiZGY3NDcwYiJ9.wFgqxxqIUhCygwefz5ee4t3p8-iajXfDpg1OZdWRoVo'
    }

    query_str = {
  "queryType": "timeseries",
  "dataSource": "summary-distinct-counts",
  "aggregations": [
    {
      "type": "HLLSketchMerge",
      "name": "unique_devices",
      "fieldName": "unique_devices",
      "lgK": "12",
      "tgtHllType": "HLL_4"
    }
  ],
  "granularity": "all",
  "postAggregations": [],
  "intervals": start_date+"T00:00:00+00:00/"+end_date+"T00:00:00+00:00",
  "filter": {
    "type": "and",
    "fields": [
      {
        "type": "or",
        "fields": [
          {
            "type": "selector",
            "dimension": "user_signin_type",
            "value": "Validated"
          },
          {
            "type": "selector",
            "dimension": "user_signin_type",
            "value": "Self-Signed-In"
          }
        ]
      },
      {
        "type": "or",
        "fields": [
          {
            "type": "selector",
            "dimension": "dimensions_pdata_id",
            "value": "prod.diksha.app"
          },
          {
            "type": "selector",
            "dimension": "dimensions_pdata_id",
            "value": "prod.diksha.portal"
          }
        ]
      }
    ]
  }
}

    jsondata = json.dumps(query_str)
    print(jsondata)
    final_list = []
    try:
        response = requests.post('http://11.4.0.53:8082/druid/v2', headers=headers, data=jsondata)

        x = response.json()
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x)
            df_pandas['Unique_Devices_Signin'] = df_pandas['result'].apply(lambda x:round(x['unique_devices']))
            df_pandas['Date'] = df_pandas['timestamp'].str.replace("T00:00:00.000Z"," ")
            df_slice          = df_pandas[['Date','Unique_Devices_Signin']]
            return df_slice
    except Exception as e:

        print(e)

df1 = Unique_Devices()
df2 = User_Sigin_Type()
df_merge = df1.merge(df2,on=['Date'],how='inner')
#df_merge.to_csv(output_path)
df_merge.to_csv("/home/analytics/lavanya/diksha_infra/enrol/day_login.csv")

def copy_gsheet():
 df = pd.read_csv('/home/analytics/lavanya/diksha_infra/enrol/day_login.csv')
 print(df)
 #df = df.head(-1);
 #df = df.drop(tail(1));
 #print(df)
 my_list = df.values.tolist()
 columns_list = df.columns.tolist()
 creds = ServiceAccountCredentials.from_json_keyfile_name('/home/analytics/lavanya/daily_reports/client_secrete.json')
 client = gspread.authorize(creds)
 print(client)
 sheet1 = client.open("TPD Results").worksheet("day_login")
 length_check_one=sheet1.get_all_values()
 if(len(length_check_one)==0):
   sheet1.insert_row(columns_list,1)
 xi=sheet1.get_all_values()
 zi=len(xi)
 for w in my_list:
    print('w::::::::::::::w',w)
    zi+=1
    print('zi:::::::::::',zi)
    sheet1.insert_row(w,zi)

copy_gsheet()
