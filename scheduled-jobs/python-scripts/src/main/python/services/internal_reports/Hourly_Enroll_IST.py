import requests
import os
import csv
import json
import glob
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import date, timedelta,datetime
from dateutil import tz
date_work = date.today()
print(date_work)
start_dates = date_work
end_dates   = date_work + timedelta(days=1)
start_date = start_dates.strftime("%Y-%m-%d")+"T00:00:00+00:00"
end_date   = end_dates.strftime("%Y-%m-%d")+"T00:00:00+00:00"

output_dir = "/home/analytics/lavanya/daily_reports/Enrollment/"
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
    "period": "PT1H"
  },
  "postAggregations": [],
  "intervals": start_date+"/"+end_date,
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
filename_two = output_dir+"ist_file.csv"
with open(filename_two, 'a') as f:
    writer = csv.writer(f)
    if (os.stat(filename_two).st_size == 0):
        writer.writerow(header_list_two)
    writer.writerows(data_list_two)

def copy_gsheet():
 df = pd.read_csv('/home/analytics/lavanya/daily_reports/Enrollment/ist_file.csv')
 print(df)
 df1 = df.sort_values('Enrollment',ascending=False).drop_duplicates('Timestamp').sort_index()

 df1.to_csv('/home/analytics/lavanya/daily_reports/Enrollment/drop_results.csv',index=False)

 path = glob.glob("/home/analytics/lavanya/daily_reports/Enrollment/drop_results.csv")

 tab_name = "Final_Result"
 try:
     SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive',
               'https://spreadsheets.google.com/feeds']

     creds = ServiceAccountCredentials.from_json_keyfile_name(
         '/home/analytics/lavanya/daily_reports/client_secrete.json', SCOPES)

     client = gspread.authorize(creds)
     spreadsheetId = '1MbrJxoWMXLiLufO3GdQIA1UGXifvYe5uqMbo0jQa8Uc'  # Please set spreadsheet ID.
     sheetName = tab_name  # Please set sheet name you want to put the CSV data.
     csvFile = path[0]  # Please set the filename and path of csv file.
     print('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::', path[0])

     sh = client.open_by_key(spreadsheetId)
     sh.values_update(
         sheetName,
         params={'valueInputOption': 'USER_ENTERED'},
         body={'values': list(csv.reader(open(csvFile)))}
     )

 except Exception as e:
     print(e)

copy_gsheet()
