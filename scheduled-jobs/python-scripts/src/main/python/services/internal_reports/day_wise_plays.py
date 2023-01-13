import requests
import os
import json
import pandas as pd
from datetime import date, timedelta
import numpy as np
import gspread

from oauth2client.service_account import ServiceAccountCredentials

output_path = "/home/analytics/lavanya/diksha_infra/plays/day_plays.csv"

todays_date = date.today()
tomm_date = date.today() - timedelta(days=1)
start_date = tomm_date.strftime("%Y-%m-%d")
end_date = todays_date.strftime("%Y-%m-%d")


os.environ['TZ'] = 'UTC'


def Play_Analysis():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }

    query_str = {
  "queryType": "topN",
  "dataSource": "summary-rollup-syncts",
  "aggregations": [
    {
      "fieldName": "total_count",
      "fieldNames": [
        "total_count"
      ],
      "type": "longSum",
      "name": "SUM(total_count)"
    },
    {
      "fieldName": "total_time_spent",
      "fieldNames": [
        "total_time_spent"
      ],
      "type": "doubleSum",
      "name": "SUM(total_time_spent)"
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
  },
  "threshold": 10000,
  "metric": "SUM(total_count)",
  "dimension": "collection_type"
}

    jsondata = json.dumps(query_str)
    #print(jsondata);
    final_list = []
    try:
        response = requests.post('http://11.4.0.53:8082/druid/v2', headers=headers, data=jsondata)

        x = response.json()
        if(len(x)!=0):
         D = x[0]['timestamp'].split("T")
         
         df_pandas = pd.DataFrame(x[0]['result'])
         df_pandas['Date'] = D[0]    
         df_pandas  = df_pandas[['Date','collection_type','SUM(total_count)','SUM(total_time_spent)']]
         df_pandas.columns = ['Date','collection_type','Play','Time_Seconds']
         df_pandas['Play_Type'] = np.where(df_pandas['collection_type']=='Course','Course',np.where(df_pandas['collection_type'].isin(['TextBook','TextBookUnit']),'Textbook','Other'))
         df_play  = df_pandas.groupby(['Date','Play_Type'])['Play'].sum().reset_index()
         df_time  = df_pandas.groupby(['Date','Play_Type'])['Time_Seconds'].sum().reset_index()
         df_merge = df_play.merge(df_time,on=['Play_Type','Date'],how='inner')
         df_merge['Time_Mins']  = (df_merge['Time_Seconds']/60)
         df_merge['Time_Hours'] = (df_merge['Time_Seconds']/3600)
         df_pivot = df_merge.pivot(index='Date',columns='Play_Type',values='Play')
         df_pivot['Total_plays'] = df_pivot.values[:,:4].sum(axis =1)
         print(df_pivot)
         df_pivot.to_csv(output_path)
    except Exception as e:
        print(e)

Play_Analysis()

def copy_gsheet():
 df = pd.read_csv('/home/analytics/lavanya/diksha_infra/plays/day_plays.csv')
 print(df)
 #df = df.head(-1);
 #df = df.drop(tail(1));
 #print(df)
 my_list = df.values.tolist()
 columns_list = df.columns.tolist()
 creds = ServiceAccountCredentials.from_json_keyfile_name('/home/analytics/lavanya/daily_reports/client_secrete.json')
 client = gspread.authorize(creds)
 print(client)
 sheet1 = client.open("DIKSHA Day wise - Infra and Consumption - Key Metrics").worksheet("Daily - Plays")
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
