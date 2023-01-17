#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "Tibil Solutions"
__version__ = "1.0.0"
__start__ = "2020-06-01"
__end__ = "NA"
__status__ = "Production"

import requests
import os
import sys
import time
import csv
import json

date = sys.argv[1]
start_date = sys.argv[2]
end_date = sys.argv[3]
output_dir = "/home/nithin/Druid_Tibil/High_Level_Analysis/Engage_High_Level"

os.environ['TZ'] = 'UTC'

# Timestamp to epoch time conversion
epoch1 = int(time.mktime(time.strptime(start_date, "%Y-%m-%d %H:%M:%S"))) * 1000
epoch2 = int(time.mktime(time.strptime(end_date, "%Y-%m-%d %H:%M:%S"))) * 1000

# Adding quotes to dates to make it query usable stuff
quoted_start_date = f"'{start_date}'"
quoted_end_date = f"'{end_date}'"


def DAU():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices who reached on onboarding
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid"='sunbird.app' AND "context_pdata_id" = 'prod.diksha.app' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date;

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Launched():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app' AND "context_env"='home' AND "eid"='INTERACT' AND "edata_type"='OTHER' AND "edata_pageid"='splash' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def New_Users():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app' AND "context_env"='onboarding' AND "eid"='IMPRESSION' AND "edata_type"='view' AND "edata_pageid"='onboarding-language-setting' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Unique_Device_Session():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of sessions of user
    query_str = '''SELECT COUNT(DISTINCT "context_sid") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Total_Device_Session():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of sessions of user
    query_str = '''SELECT COUNT("context_sid") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Saw_User_Type():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app' AND "context_env"='onboarding' AND "eid"='IMPRESSION' AND "edata_type"='view' AND "edata_pageid"='user-type-selection' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Saw_Permission_Screen():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app' AND "eid"='IMPRESSION' AND "edata_type"='view' AND "edata_pageid"= 'permission' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Saw_Onboarding():
    
    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app' AND "context_env"='onboarding' AND "eid"='IMPRESSION' AND "edata_type"='view' AND "edata_pageid"='profile-settings' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e));

def Onboarding_Finished():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app' AND "eid"='INTERACT' AND "edata_type"='OTHER' AND "edata_subtype"='profile-attribute-population' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e));


def Finished_Manual_Onboarding():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"='INTERACT' AND "edata_type"='OTHER' AND "edata_subtype" = 'profile-attribute-population' AND "edata_pageid"='profile-settings' AND "context_env" = 'onboarding' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e));


def Saw_Location_Ask():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"='IMPRESSION' AND "edata_type"='view' AND "edata_pageid"='district-mapping' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Submitted_Location_Ask():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "edata_pageid" ='district-mapping' AND "edata_type" = 'location-changed' AND "edata_id" = 'submit-clicked' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Landed_On_Library():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices  where users landed on library page
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"='IMPRESSION' AND "context_env"='home' AND "edata_pageid" IN ('library','resources') AND "edata_type" = 'view' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Unique_Tapped_On_Textbook():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users tapped on textbook
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" ='home' AND "edata_pageid" = 'library' AND "edata_type" = 'TOUCH' AND "edata_subtype" = 'content-clicked'  AND "object_type" IN ('TextBook','Digital Textbook') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Total_Tapped_On_Textbook():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users tapped on textbook
    query_str = '''SELECT COUNT("context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" ='home' AND "edata_pageid" = 'library' AND "edata_type" = 'TOUCH' AND "edata_subtype" = 'content-clicked'  AND "object_type" IN ('TextBook','Digital Textbook') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Unique_Tapped_Content():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users tapped on content
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" ='home' AND "edata_pageid" = 'collection-detail' AND "edata_type" = 'TOUCH' AND "edata_subtype" = 'content-clicked'  AND "object_type" IN ('Resource','Explanation Content','Learning Resource','Practice Question Set','eTextbook','Teacher Resource','Course Assessment') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Total_Tapped_Content():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users tapped on content
    query_str = '''SELECT COUNT("context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" ='home' AND "edata_pageid" = 'collection-detail' AND "edata_type" = 'TOUCH' AND "edata_subtype" = 'content-clicked'  AND "object_type" IN ('Resource','Explanation Content','Learning Resource','Practice Question Set','eTextbook','Teacher Resource','Course Assessment') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Unique_Tapped_Qr():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "edata_type" ='TOUCH' AND "edata_subtype" = 'tab-clicked' AND "edata_pageid" = 'qr-code-scanner' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Total_Tapped_Qr():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where app was launched
    query_str = '''SELECT COUNT("context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "edata_type" ='TOUCH' AND "edata_subtype" = 'tab-clicked' AND "edata_pageid" = 'qr-code-scanner' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))



def Saw_Result():

    #List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    #Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users saw result
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'IMPRESSION'  AND "edata_type" ='view' AND  "edata_pageid" = 'dial-code-scan-result' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Scan_UTM_Info():


    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users scanned utm info
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "edata_type" ='OTHER' AND "edata_subtype" = 'utm-info' AND "edata_pageid" = 'qr-code-scanner' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))

def Scan_Initiate():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users sacnned qr and was successfull
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'IMPRESSION'  AND "edata_type" ='view' AND "edata_subtype" = 'qr-code-scan-initiate'  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Scan_Walk_Through():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users sacnned qr and was successfull
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'IMPRESSION'  AND "edata_type" ='view' AND "edata_subtype" = 'qr-scan-walkthrough'  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))






def Scan_Success():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users sacnned qr and was successfull
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "edata_type" ='OTHER' AND "edata_subtype" = 'qr-code-scan-success'  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Scan_Cancelled():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users scanned qr but scanning failed
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "edata_type" ='OTHER' AND "edata_subtype" = 'qr-code-scan-cancelled'  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Scan_Comming_Soon():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where scan comming soon appeared
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "edata_type" ='OTHER'  AND "edata_subtype" = 'qr-code-comingsoon'  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))



def Scan_Invalid():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where scan comming soon appeared
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "edata_type" ='OTHER'  AND "edata_subtype" = 'qr-code-invalid'  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))




def Tapped_On_Play():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users tapped on the play
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND  "edata_type" = 'TOUCH' AND "edata_subtype" IN('play-online','play-from-device')  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Unique_Overall_Play():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users played content 
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id"='prod.diksha.app'  AND "eid"= 'START'  AND "edata_type" = 'content' AND "context_env" IN('contentplayer','ContentPlayer')  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Total_Overall_Play():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users played content
    query_str = '''SELECT COUNT("context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id"='prod.diksha.app'  AND "eid"= 'START'  AND "edata_type" = 'content' AND "context_env" IN ('contentplayer','ContentPlayer') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))





def Content_Play_End():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users played content 
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id"='prod.diksha.app'  AND "edata_pageid"= 'sunbird-player-Endpage'  AND "context_cdata_type" = 'PlayerLaunch' AND "eid" = 'END'  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Content_Play_Cancel():


 # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users played content 
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id"='prod.diksha.app'  AND "edata_type"= 'OTHER'  AND "context_env" = 'contentplayer' AND  "eid" = 'INTERRUPT' AND "edata_pageid" IS NOT NULL  AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))



def Download_Initiate():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users initiated download
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "edata_type" ='OTHER' AND "edata_subtype" = 'ContentDownload-Initiate' AND "context_env" = 'sdk' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Download_Cancelled():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices where users cancelled download
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "edata_type" ='OTHER' AND "edata_subtype" = 'ContentDownload-Cancel' AND "context_env" = 'sdk' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def Download_Success():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices on which download was successfull
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "edata_type" ='OTHER' AND "edata_subtype" = 'ContentDownload-Success' AND "context_env" = 'sdk' AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);
        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def search_button_clicked_unique_did():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices with search button clicked
    query_str = '''SELECT COUNT(DISTINCT "context_did")  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id"= 'prod.diksha.app' AND "eid" = 'INTERACT' AND "edata_type" = 'TOUCH'  AND (("edata_subtype" = 'search-button-clicked') OR ("edata_subtype" = 'search-buttonclicked'))   AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);

        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


def search_button_clicked_total_did():

    # List declaration to store fetched data
    date_list = [];
    count_list = [];
    date_list.append(date);

    # Header details for POST request
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    # Number of devices with search button clicked
    query_str = '''SELECT COUNT("context_did")  FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_id"= 'prod.diksha.app' AND "eid" = 'INTERACT' AND "edata_type" = 'TOUCH'  AND (("edata_subtype" = 'search-button-clicked') OR ("edata_subtype" = 'search-buttonclicked'))   AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

    data = {"query": query_str}
    jsondata = json.dumps(data);

    # Fetching data from Druid using POST request
    try:
        response = requests.post('http://11.4.3.46:8000/druid/sql', headers=headers, data=jsondata)
        x = response.json();
        if (len(x) != 0):
            count_list = list(x[0].values());
        else:
            count_list.append(0);

        if not os.path.exists(output_dir):
            os.mkdir(output_dir);
        filename = os.path.join(output_dir, sys._getframe().f_code.co_name + ".csv");

        # Storing count of devices to CSV File
        with open(filename, 'a') as f:
            writer = csv.writer(f)
            if (os.stat(filename).st_size == 0):
                writer.writerow(["date", "count"])
            writer.writerows(zip(date_list, count_list));
    except Exception as e:
        print(e.__class__.__name__ + '-' + str(e))


DAU()
Launched()
New_Users()
Unique_Device_Session()
Total_Device_Session()
Saw_User_Type()
Saw_Permission_Screen()
Saw_Onboarding()
Finished_Manual_Onboarding()
Saw_Location_Ask()
Submitted_Location_Ask()
Landed_On_Library()
Unique_Tapped_On_Textbook()
Total_Tapped_On_Textbook()
Unique_Tapped_Content()
Total_Tapped_Content()
Unique_Tapped_Qr()
Total_Tapped_Qr()
Scan_Success()
Scan_Cancelled()
Scan_Comming_Soon()
Tapped_On_Play()
Unique_Overall_Play()
Total_Overall_Play()
Download_Initiate()
Download_Cancelled()
Download_Success()
search_button_clicked_unique_did()
search_button_clicked_total_did()
Onboarding_Finished()
Saw_Result()
Scan_UTM_Info()
Scan_Invalid()
Scan_Initiate()
Content_Play_End()
Content_Play_Cancel()
