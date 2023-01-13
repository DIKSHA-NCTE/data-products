#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "Tibil Solutions"
__version__ = "1.0.0"
__start__ = "2020-10-12"
__end__ = "NA"
__status__ = "Production"
import requests
import os
import sys
import time
import datetime
import json
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from functools import reduce



date = sys.argv[1]
start_date = sys.argv[2];
end_date = sys.argv[3];

os.environ['TZ'] = 'UTC';

# Adding quotes to dates to make it query usable stuff
quoted_start_date = f"'{start_date}'"
quoted_end_date = f"'{end_date}'"


def Unique_Devices_Portal():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Portal" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            my_dict = {'Date':[date],'Unique_Device_Portal':x[0]['Unique_Device_Portal']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Portal': 0};
            df_pandas = pd.DataFrame(my_dict);
        return  df_pandas;


    except Exception as e:
        print(e);

def Unique_Device_Scan_Non_Diksha():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Scan_Non_Diksha" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'IMPRESSION' AND "edata_pageid" = 'get-dial' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            my_dict = {'Date':[date],'Unique_Device_Scan_Non_Diksha':x[0]['Unique_Device_Scan_Non_Diksha']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Scan_Non_Diksha': 0};
            df_pandas = pd.DataFrame(my_dict);
        return  df_pandas;


    except Exception as e:
        print(e);

def Unique_Device_Play_Via_Scan():
    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Play_Via_Scan" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'START' AND "edata_type" = 'content' AND "edata_mode" = 'play' AND "context_cdata_type" = 'DialCode' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Play_Via_Scan': x[0]['Unique_Device_Play_Via_Scan']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Play_Via_Scan': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);

def Unique_Device_Play_Content():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Device_Play_Content" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "eid" = 'START' AND "edata_type" = 'content' AND "edata_mode" = 'play'  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Device_Play_Content': x[0]['Unique_Device_Play_Content']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Unique_Device_Play_Content': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);


def Total_Device_Play_Content():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT("context_did") AS "Total_Device_Play_Content" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND  "eid" = 'START' AND "edata_type" = 'content' AND "edata_mode" = 'play'  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Total_Device_Play_Content': x[0]['Total_Device_Play_Content']};
            df_pandas = pd.DataFrame(my_dict);
            print(df_pandas);
        else:
            my_dict = {'Date': [date], 'Total_Device_Play_Content': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);

def Unique_Mobile_Device():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Mobile_Device" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "context_cdata_id" IN ('Mobile', 'mobile', 'tab', 'Tab') AND "context_cdata_type" IN ('Device', 'DeviceType') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;
    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        print(x);
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Mobile_Device': x[0]['Unique_Mobile_Device']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_Mobile_Device': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);

def Unique_Desktop_Device():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") AS "Unique_Desktop_Device" FROM "druid"."telemetry-events-syncts" WHERE  "context_pdata_id" = 'prod.diksha.portal' AND "context_cdata_id" IN ('Desktop', 'pc', 'Pc', 'PC') AND "context_cdata_type" IN ('Device', 'DeviceType') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;
    data = {"query": query_str}
    jsondata = json.dumps(data);


    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        print(x);
        if (len(x) != 0):
            my_dict = {'Date': [date], 'Unique_Desktop_Device': x[0]['Unique_Desktop_Device']};
            df_pandas = pd.DataFrame(my_dict);
        else:
            my_dict = {'Date': [date], 'Unique_Desktop_Device': 0};
            df_pandas = pd.DataFrame(my_dict);
        return df_pandas;


    except Exception as e:
        print(e);

df1 = Unique_Devices_Portal();
df2 = Unique_Mobile_Device();
df3 = Unique_Desktop_Device();
df4 = Unique_Device_Scan_Non_Diksha();
df5 = Unique_Device_Play_Via_Scan();
df6 = Unique_Device_Play_Content();
df7 = Total_Device_Play_Content();

my_dfs = [df1,df2,df3,df4,df5,df6,df7];

df_merge = reduce(lambda left,right: pd.merge(left,right,on='Date',how='inner'),my_dfs);
header_list  = list(df_merge.columns);
overall_snapshot = df_merge.values.tolist();
try:
            creds = ServiceAccountCredentials.from_json_keyfile_name('/home/nagendra/tibil/code/Igot_Final_Code/my_file.json')

            client = gspread.authorize(creds)

            # Find a workbook by name and open the first sheet
            # Make sure you use the right name here.
            sheet1 = client.open("Diksha Portal Funnel Analysis").worksheet("Simple_Portal_Funnel")
            length_check_one = sheet1.get_all_values();
            if (len(length_check_one) == 0):
                sheet1.insert_row(header_list, 1);
            xi = sheet1.get_all_values();
            zi = len(xi);
            for w in overall_snapshot:
                zi += 1;
                sheet1.insert_row(w, zi);
except Exception as e:
            print(e);
