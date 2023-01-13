import requests
import os
import csv
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import date, timedelta, datetime
from dateutil import tz

date_work = date.today()
print(date_work)
start_dates = date_work
end_dates = date.today() - timedelta(days=1)
start_date = end_dates.strftime("%Y-%m-%d")
print(start_date)
end_date = date_work.strftime("%Y-%m-%d")
print(end_date)

output_dir = "/home/analytics/lavanya/diksha_infra/test/"
if not os.path.exists(output_dir):
    os.mkdir(output_dir)

indiazone = tz.gettz('Asia/Kolkata')

def Course_Enrollment():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }

    query_str ={
  "queryType": "timeseries",
  "dataSource": "tpd-hourly-rollup-syncts",
  "aggregations": [
    {
      "type": "longSum",
      "name": "sum__total_count",
      "fieldName": "total_count"
    }
  ],
  "granularity": {
    "type": "period",
    "timeZone": "IST",
    "period": "P1D"
  },
  "postAggregations": [],
  "intervals": start_date+"T00:00:00+00:00/"+end_date+"T00:00:00+00:00",
  "filter": {
    "type": "selector",
    "dimension": "edata_type",
    "value": "enrol"
  }
}
    jsondata = json.dumps(query_str)
    print(jsondata)
    final_list = []
    try:
        response = requests.post('http://11.4.0.53:8082/druid/v2', headers=headers, data=jsondata)

        x = response.json()
        print(x)
        ist_time  = []
        utc_time  = []
        enrol_track = []
        my_dict = {}

        if(len(x)!=0):
            for i in x:
                time_val = i['timestamp'].replace(".000+05:30","")
                time_val = datetime.strptime(time_val,'%Y-%m-%dT%H:%M:%S')
                utc_time.append(time_val)
                enrol_track.append(i['result']['sum__total_count'])
            my_dict['Timestamp']  = utc_time
            my_dict['Enrollment'] = enrol_track
        else:
            my_dict['Timestamp'] = None
            my_dict['Enrollment'] = None
        df_pandas = pd.DataFrame(my_dict)
        print(df_pandas)
        return df_pandas

    except Exception as e:
        print(e)

df_data = Course_Enrollment()
header_list_two = list(df_data.columns)
data_list_two   = df_data.values.tolist()
filename_two = output_dir+"daily_enrol_output.csv"
with open(filename_two, 'w') as f:
    writer = csv.writer(f)
    if (os.stat(filename_two).st_size == 0):
        writer.writerow(header_list_two)
    writer.writerows(data_list_two)

def copy_gsheet():
 df = pd.read_csv('/home/analytics/lavanya/diksha_infra/test/daily_enrol_output.csv')
 print(df)
 df = df.head(-1)
 #df = df.drop(tail(1));
 print(df)
 my_list = df.values.tolist()
 columns_list = df.columns.tolist()
 creds = ServiceAccountCredentials.from_json_keyfile_name('/home/analytics/lavanya/daily_reports/client_secrete.json')
 client = gspread.authorize(creds)
 print(client)
 sheet1 = client.open("TPD Results").worksheet("test_enrol")
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
