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
import datetime
import csv
import json

date = sys.argv[1]
start_date = sys.argv[2];
end_date = sys.argv[3];
output_dir = "/home/nithin/Druid_Tibil/High_Level_Analysis/New_Metric_Related_To_Play"

os.environ['TZ'] = 'UTC';

# Timestamp to epoch time conversion
epoch1 = int(time.mktime(time.strptime(start_date, "%Y-%m-%d %H:%M:%S"))) * 1000
epoch2 = int(time.mktime(time.strptime(end_date, "%Y-%m-%d %H:%M:%S"))) * 1000

# Adding quotes to dates to make it query usable stuff
quoted_start_date = f"'{start_date}'"
quoted_end_date = f"'{end_date}'"


def Unique_Tapped_On_Textbook_New():

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
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" IN('home','search') AND "edata_pageid" IN('library','category-results') AND "edata_type" IN('TOUCH','select-content') AND ("edata_subtype" = 'content-clicked'OR "edata_subtype" IS NULL)  AND "object_type" IN ('TextBook','Digital Textbook') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

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


def Total_Tapped_On_Textbook_New():

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
    query_str = '''SELECT COUNT("context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" IN('home','search') AND "edata_pageid" IN('library','category-results') AND "edata_type" IN('TOUCH','select-content') AND ("edata_subtype" = 'content-clicked' OR "edata_subtype" IS NULL) AND "object_type" IN ('TextBook','Digital Textbook') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

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


def Unique_Tapped_Content_New():

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
    query_str = '''SELECT COUNT(DISTINCT "context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" IN('home','search') AND "edata_pageid" IN('collection-detail','category-results') AND "edata_type" IN('TOUCH','select-content') AND ("edata_subtype" = 'content-clicked' OR "edata_subtype" IS NULL) AND "object_type" IN ('Resource','Explanation Content','Learning Resource','Practice Question Set','eTextbook','Teacher Resource','Course Assessment') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

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


def Total_Tapped_Content_New():

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
    query_str = '''SELECT COUNT("context_did") FROM "druid"."telemetry-events-syncts" WHERE "context_pdata_pid" = 'sunbird.app' AND "context_pdata_id"='prod.diksha.app'  AND "eid"= 'INTERACT'  AND "context_env" IN('home','search') AND "edata_pageid" IN('collection-detail','category-results') AND "edata_type" IN('TOUCH','select-content') AND ("edata_subtype" = 'content-clicked' OR "edata_subtype" IS NULL) AND "object_type" IN ('Resource','Explanation Content','Learning Resource','Practice Question Set','eTextbook','Teacher Resource','Course Assessment') AND "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date

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


Unique_Tapped_On_Textbook_New()
Total_Tapped_On_Textbook_New()
Unique_Tapped_Content_New()
Total_Tapped_Content_New()

