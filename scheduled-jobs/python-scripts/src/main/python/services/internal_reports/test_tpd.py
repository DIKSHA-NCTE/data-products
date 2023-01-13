import requests
import pandas as pd
from datetime import date, timedelta
from functools import reduce
import numpy as np
import glob
import csv
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

todays_date = date.today()
tomm_date = date.today() + timedelta(days=1)
start_date = todays_date.strftime("%Y-%m-%d") + "T00:00:00+05:30"
end_date = tomm_date.strftime("%Y-%m-%d") + "T00:00:00+05:30"
output_path = "/home/analytics/lavanya/daily_reports/tpd_dashboard/tpd_metrics_date.csv"


def OTP_Login():
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }

    query_str = {
        "queryType": "topN",
        "dataSource": "tpd-hourly-rollup-syncts",
        "aggregations": [
            {
                "fieldName": "total_count",
                "fieldNames": [
                    "total_count"
                ],
                "type": "longSum",
                "name": "SUM(total_count)"
            }
        ],
        "granularity": {
            "type": "period",
            "period": "PT2H",
            "timeZone": "IST"
        },
        "postAggregations": [],
        # "intervals": start_date+"/"+end_date,
        "intervals": "2021-03-09T00:00:00+00:00/" + end_date,
        "filter": {
            "type": "selector",
            "dimension": "edata_pageid",
            "value": "otp"
        },
        "threshold": 50000,
        "metric": "SUM(total_count)",
        "dimension": "edata_id"
    }
    jsondata = json.dumps(query_str)
    print(jsondata)
    global_list = []
    try:
        response = requests.post('http://11.4.0.53:8082/druid/v2', headers=headers, data=jsondata)

        x = response.json()

        if (len(x) != 0):
            for i in x:

                for j in range(len(i['result'])):
                    i['result'][j]['timestamp'] = i['timestamp']

                global_list.extend(i['result'])
        df_pandas = pd.DataFrame(global_list)
        df_pandas = df_pandas[['timestamp', 'edata_id', 'SUM(total_count)']]
        df_pandas.columns = ['Date', 'edata_id', 'Count']
        df_agg = df_pandas.set_index(['Date', 'edata_id'])['Count'].unstack().reset_index()
        df_agg.fillna('0', inplace=True)
        return df_agg
    except Exception as e:
        print(e)


def course_play():
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }

    query_str = {
        "queryType": "timeseries",
        "dataSource": "tpd-hourly-rollup-syncts",
        "aggregations": [
            {
                "fieldName": "total_count",
                "fieldNames": [
                    "total_count"
                ],
                "type": "longSum",
                "name": "SUM(total_count)"
            }
        ],
        "granularity": {
            "type": "period",
            "period": "PT2H",
            "timeZone": "IST"
        },
        "postAggregations": [],
        # "intervals": "2021-02-02T00:00:00+00:00/2021-02-04T00:00:00+00:00",
        "intervals": "2021-03-09T00:00:00+00:00/" + end_date,
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "collection_type",
                    "value": "Course"
                },
                {
                    "type": "and",
                    "fields": [
                        {
                            "type": "selector",
                            "dimension": "edata_type",
                            "value": "content"
                        },
                        {
                            "type": "and",
                            "fields": [
                                {
                                    "type": "selector",
                                    "dimension": "edata_mode",
                                    "value": "play"
                                },
                                {
                                    "type": "selector",
                                    "dimension": "eid",
                                    "value": "START"
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
    try:
        response = requests.post('http://11.4.0.53:8082/druid/v2', headers=headers, data=jsondata)

        x = response.json()
        if (len(x) != 0):
            df_pandas = pd.DataFrame(x)
            df_pandas['Course_Play'] = df_pandas['result'].apply(lambda x: x['SUM(total_count)'])
            df_slice = df_pandas[['timestamp', 'Course_Play']]
            df_slice.columns = ['Date', 'Course_Play']
            return df_slice
    except Exception as e:
        print(e)


def registration():
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }

    query_str = {
        "queryType": "timeseries",
        "dataSource": "tpd-hourly-rollup-syncts",
        "aggregations": [
            {
                "fieldName": "total_count",
                "fieldNames": [
                    "total_count"
                ],
                "type": "longSum",
                "name": "SUM(total_count)"
            }
        ],
        "granularity": {
            "type": "period",
            "period": "PT2H",
            "timeZone": "IST"
        },
        "postAggregations": [],
        # "intervals":"2021-02-02T00:00:00+00:00/2021-02-04T00:00:00+00:00",
        "intervals": "2021-03-09T00:00:00+00:00/" + end_date,
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "first_time_user",
                    "value": "true"
                },
                {
                    "type": "and",
                    "fields": [
                        {
                            "type": "selector",
                            "dimension": "eid",
                            "value": "IMPRESSION"
                        },
                        {
                            "type": "and",
                            "fields": [
                                {
                                    "type": "selector",
                                    "dimension": "edata_type",
                                    "value": "page-loaded"
                                },
                                {
                                    "type": "and",
                                    "fields": [
                                        {
                                            "type": "selector",
                                            "dimension": "edata_pageid",
                                            "value": "splash"
                                        },
                                        {
                                            "type": "selector",
                                            "dimension": "context_env",
                                            "value": "onboarding"
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
    try:
        response = requests.post('http://11.4.0.53:8082/druid/v2', headers=headers, data=jsondata)

        x = response.json()
        if (len(x) != 0):
            df_pandas = pd.DataFrame(x)
            df_pandas['Registration_Count'] = df_pandas['result'].apply(lambda x: x['SUM(total_count)'])
            df_slice = df_pandas[['timestamp', 'Registration_Count']]
            df_slice.columns = ['Date', 'Registration_Count']
            return df_slice
    except Exception as e:
        print(e)


def logins_init():
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }

    query_str = {
        "queryType": "topN",
        "dataSource": "tpd-hourly-rollup-syncts",
        "aggregations": [
            {
                "type": "HLLSketchMerge",
                "name": "unique_actors",
                "fieldName": "unique_users",
                "lgK": "12",
                "tgtHllType": "HLL_4"
            }
        ],
        "granularity": {
            "type": "period",
            "period": "PT2H",
            "timeZone": "IST"
        },
        # "intervals": "2021-02-02T00:00:00+00:00/2021-02-04T00:00:00+00:00",
        "intervals": "2021-03-09T00:00:00+00:00/" + end_date,
        "metric": "unique_actors",
        "dimension": "user_signin_type",
        "threshold": 10000
    }

    jsondata = json.dumps(query_str)
    print(jsondata)
    global_list = []
    try:
        response = requests.post('http://11.4.0.53:8082/druid/v2', headers=headers, data=jsondata)

        x = response.json()
        if (len(x) != 0):
            for i in x:

                for j in range(len(i['result'])):
                    i['result'][j]['timestamp'] = i['timestamp']

                global_list.extend(i['result'])
        df_pandas = pd.DataFrame(global_list)
        df_pandas = df_pandas[['timestamp', 'user_signin_type', 'unique_actors']]
        df_pandas.columns = ['Date', 'user_signin_type', 'Count']
        df_pandas['Count'] = df_pandas['Count'].apply(lambda x: round(x))
        df_pandas['user_signin_type'] = np.where(df_pandas['user_signin_type'].isna(), 'Not_Available_Signin_Type',
                                                 df_pandas['user_signin_type'])
        df_agg = df_pandas.set_index(['Date', 'user_signin_type'])['Count'].unstack().reset_index()
        df_agg.fillna('0', inplace=True)
        return df_agg
    except Exception as e:
        print(e)


df1 = OTP_Login()
df2 = course_play()
df3 = registration()
df4 = logins_init()

li = [df1, df2, df3, df4]
frame = reduce(lambda x, y: x.merge(y, on=['Date'], how='outer'), li)
frame.to_csv(output_path, index=0)


def copy_gsheet():
    df = pd.read_csv('/home/analytics/lavanya/daily_reports/tpd_dashboard/tpd_metrics_date.csv')
    print(df)
    df1 = df.sort_values('Self-Signed-In', ascending=False).drop_duplicates('Date').sort_index()

    df1.to_csv('/home/analytics/lavanya/daily_reports/tpd_dashboard/drop_results1.csv', index=False)

    path = glob.glob("/home/analytics/lavanya/daily_reports/tpd_dashboard/drop_results1.csv")

    tab_name = "TPD_RESULT"
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive',
                  'https://spreadsheets.google.com/feeds']

        creds = ServiceAccountCredentials.from_json_keyfile_name(
            '/home/analytics/lavanya/daily_reports/client_secrete.json', SCOPES)

        client = gspread.authorize(creds)
        spreadsheetId = '1Wtca1lQWjhtEqg60TcJA-MAq-fu8_iut5Tpsi4RryvQ'  # Please set spreadsheet ID.
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
