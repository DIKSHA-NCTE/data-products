import requests
import os
import sys
import time
import json
import pandas as pd
from functools import reduce
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials

date       = sys.argv[1]
start_date = sys.argv[2];
end_date   = sys.argv[3];
excel_file = "Tara usage metrics";

os.environ['TZ'] = 'UTC';

# Adding quotes to dates to make it query usable stuff
quoted_start_date = f"'{start_date}'"
quoted_end_date = f"'{end_date}'"

def upload_gsheet(excel_file,sheet_name,df_pivot):

    try:

        header_list       = list(df_pivot.columns);
        overall_snapshot  = df_pivot.values.tolist();

        creds = ServiceAccountCredentials.from_json_keyfile_name(
            '/home/nithin/druid_rajesh/Portal_Analysis/my_file.json')

        client = gspread.authorize(creds)

        # Find a workbook by name and open the first sheet
        # Make sure you use the right name here.
        sheet1 = client.open(excel_file).worksheet(sheet_name)
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

def Total_Users_Clicked_On_Tara_Portal():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    query_str = '''SELECT COUNT("context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'dikshavani.botclient' and "eid" = 'INTERACT' and "edata_type" = 'START' and "edata_id" = 'step1' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x);
            df_pandas['Date'] = date;
            df_slice          = df_pandas[['Date','Count']]
        else:
            df_slice = pd.DataFrame({'Date':[date],'Count':[0]});
        return  df_slice

    except Exception as e:
        print(e);

def Total_Users_Clicked_On_Tara_Whatsapp():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    query_str = '''SELECT COUNT("context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'dikshavani.botclient' AND "context_env" = 'diksha.whatsapp' and "eid" = 'INTERACT' AND "edata_type" ='START' AND "edata_id" ='step1' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x);
            df_pandas['Date'] = date;
            df_slice          = df_pandas[['Date','Count']]
        else:
            df_slice = pd.DataFrame({'Date':[date],'Count':[0]});
        return  df_slice

    except Exception as e:
        print(e);
def Total_Users_Clicked_On_Tara_Portal_One():

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT("context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id" = 'prod.diksha.portal' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            df_pandas = pd.DataFrame(x);
            df_pandas['Date'] = date;
            df_slice = df_pandas[['Date', 'Count']]
        else:
            df_slice = pd.DataFrame({'Date': [date], 'Count': [0]});
        return df_slice

    except Exception as e:
        print(e);
def Total_Users_Clicked_On_Tara_Whatsapp_One():

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    query_str = '''SELECT COUNT("context_did") AS "Count"  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'dikshavani.botclient' AND "context_env" = 'diksha.whatsapp' and "eid" = 'INTERACT' AND "edata_type" ='START' AND "edata_id" ='step1' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date
    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if(len(x)!=0):
            df_pandas = pd.DataFrame(x);
            df_pandas['Date'] = date;
            df_slice          = df_pandas[['Date','Count']]
        else:
            df_slice = pd.DataFrame({'Date':[date],'Count':[0]});
        return  df_slice

    except Exception as e:
        print(e);

df1 = Total_Users_Clicked_On_Tara_Portal();
df2 = Total_Users_Clicked_On_Tara_Whatsapp();
df3 = Total_Users_Clicked_On_Tara_Portal_One();
df4 = Total_Users_Clicked_On_Tara_Whatsapp_One();

df1["Source"]="Portal"
df1['Metric']="Total_Users_Clicked_On_Tara"
df2["Source"]="Whatsapp"
df2['Metric']="Total_Users_Clicked_On_Tara"
df3["Source"]="Portal"
df3['Metric']= "Total_Users"
df4['Metric']="Total_Users"
df4["Source"]="Whatsapp"

my_dfs = [df1,df2,df3,df4];
df_merge = pd.concat(my_dfs, axis = 0, ignore_index= True)
df_pivot = df_merge.set_index(['Date', 'Source', 'Metric'])['Count'].unstack().reset_index();
df_pivot.fillna(0, inplace=True);
df_pivot.sort_values("Source", axis=0, ascending=False, inplace=True, na_position='last')
print(df_pivot)
upload_gsheet("Tara usage metrics","NCERT Metrics",df_pivot)
