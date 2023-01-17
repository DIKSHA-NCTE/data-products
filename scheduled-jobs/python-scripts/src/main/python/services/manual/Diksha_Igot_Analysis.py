import requests
import os
import sys
import json
import pandas as pd
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials


todays_date = date.today()
tomm_date = date.today() - timedelta(days=1)
# start_date = tomm_date.strftime("%Y-%m-%d");
# end_date = todays_date.strftime("%Y-%m-%d");
start_date = sys.argv[1]
end_date   = sys.argv[2] 

os.environ['TZ'] = 'UTC'


def Diksha_Play():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }

    query_str = {
  "queryType": "timeseries",
  "dataSource": "summary-rollup-syncts",
  "aggregations": [
    {
      "type": "longSum",
      "name": "sum__total_count",
      "fieldName": "total_count"
    },
    {
      "type": "doubleSum",
      "name": "sum__total_time_spent",
      "fieldName": "total_time_spent"
    }
  ],
  "granularity": "all",
  "postAggregations": [],
  "intervals": start_date+"/"+end_date,
  "filter": {
    "type": "and",
    "fields": [
      {
        "type": "not",
        "field": {
          "type": "selector",
          "dimension": "content_created_for",
          "value": "0129894906672087041553"
        }
      },
      {
        "type": "and",
        "fields": [
          {
            "type": "selector",
            "dimension": "dimensions_type",
            "value": "content"
          },
          {
            "type": "and",
            "fields": [
              {
                "type": "selector",
                "dimension": "dimensions_mode",
                "value": "play"
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
                  },
                  {
                    "type": "selector",
                    "dimension": "dimensions_pdata_id",
                    "value": "prod.diksha.desktop"
                  }
                ]
              }
            ]
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

        if (len(x) != 0):
            result_list = x[0]['result']
            my_list = []
            my_list.append(result_list)
            df_pandas = pd.DataFrame(my_list)
            df_pandas.columns = ['Diksha_Play','Diksha_Time_Seconds']
            df_pandas['Diksha_Time_Hours'] = (df_pandas['Diksha_Time_Seconds']/3600)
            df_pandas['Diksha_Time_Hours']= df_pandas['Diksha_Time_Hours'].round(2)
            df_pandas['Date'] = start_date
            df_pandas = df_pandas[['Date','Diksha_Play','Diksha_Time_Hours']]
            return  df_pandas
        else:
            df_pandas = pd.DataFrame({'Date':[start_date],'Diksha_Play':[0],'Diksha_Time_Hours':[0]})
            return  df_pandas
    except Exception as e:
        print(e)

def Igot_Play():
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }

    query_str = {
  "queryType": "timeseries",
  "dataSource": "summary-rollup-syncts",
  "aggregations": [
    {
      "type": "longSum",
      "name": "sum__total_count",
      "fieldName": "total_count"
    },
    {
      "type": "doubleSum",
      "name": "sum__total_time_spent",
      "fieldName": "total_time_spent"
    }
  ],
  "granularity": "all",
  "postAggregations": [],
  "intervals": start_date+"/"+end_date,
  "filter": {
    "type": "and",
    "fields": [
      {
        "type": "selector",
        "dimension": "content_created_for",
        "value": "0129894906672087041553"
      },
      {
        "type": "and",
        "fields": [
          {
            "type": "selector",
            "dimension": "dimensions_type",
            "value": "content"
          },
          {
            "type": "and",
            "fields": [
              {
                "type": "selector",
                "dimension": "dimensions_mode",
                "value": "play"
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
                  },
                  {
                    "type": "selector",
                    "dimension": "dimensions_pdata_id",
                    "value": "prod.diksha.desktop"
                  }
                ]
              }
            ]
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

        if (len(x) != 0):
            result_list = x[0]['result']
            my_list = []
            my_list.append(result_list)
            df_pandas = pd.DataFrame(my_list)
            df_pandas.columns = ['Igot_Play', 'Igot_Time_Seconds']
            df_pandas['Igot_Time_Hours'] = (df_pandas['Igot_Time_Seconds'] / 3600)
            df_pandas['Igot_Time_Hours'] = df_pandas['Igot_Time_Hours'].round(2)
            df_pandas['Date'] = start_date
            df_pandas = df_pandas[['Date', 'Igot_Play', 'Igot_Time_Hours']]
            return df_pandas
        else:
            df_pandas = pd.DataFrame({'Date': [start_date], 'Igot_Play': [0], 'Igot_Time_Hours': [0]})
            return df_pandas
    except Exception as e:
        print(e)


df_diksha = Diksha_Play()
df_igot   = Igot_Play()
df_merge  = df_diksha.merge(df_igot,on=['Date'],how='inner')
df_merge  = df_merge[['Date','Diksha_Play','Igot_Play','Diksha_Time_Hours','Igot_Time_Hours']]
header_list = list(df_merge.columns)
overall_snapshot = df_merge.values.tolist()

try:
   creds = ServiceAccountCredentials.from_json_keyfile_name('/home/nithin/druid_rajesh/Portal_Analysis/my_file.json')

   client = gspread.authorize(creds)

   # Find a workbook by name and open the first sheet
   # Make sure you use the right name here.
   sheet1 = client.open("Mauka_Two_Report_Final").worksheet("Diksha_Vs_Igot")
   length_check_one=sheet1.get_all_values()
   if(len(length_check_one)==0):
    sheet1.insert_row(header_list,1)
   xi=sheet1.get_all_values()
   zi=len(xi)
   for w in overall_snapshot:
    zi+=1
    sheet1.insert_row(w,zi)
except Exception as e:
    print(e)
