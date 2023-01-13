import csv
import glob
import json
import sys

import requests
from pyspark.sql import functions as F
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pyspark.sql import SparkSession
from datetime import timedelta, date
from pyspark.sql.types import *

date_work = date.today()
print(date_work)
end_dates = date_work
start_dates = date_work - timedelta(days=1)
start_date = start_dates.strftime("%Y-%m-%d") + "T00:00:00+00:00"
end_date = end_dates.strftime("%Y-%m-%d") + "T00:00:00+00:00"
#####################################################################

todays_date = date.today()
Date = todays_date.strftime("%Y-%m-%d")

Date_split = Date.split('-')
Date = Date_split[0] + Date_split[1] + Date_split[2]
file_name = 'summary-report-' + Date + '.csv'
print(file_name)

tenant_name = sys.argv[1]

event_list = []


def analysis_work(tenant_name):
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    query_str = {
        "queryType": "groupBy",
        "dataSource": "collection-summary-snapshot",
        "dimensions": [
            "content_org",
            "collection_id",
            "collection_name",
            "batch_id",
            "batch_start_date",
            "batch_end_date"
        ],
        "aggregations": [
            {
                "fieldName": "total_enrolment",
                "fieldNames": [
                    "total_enrolment"
                ],
                "type": "longSum",
                "name": "SUM(total_enrolment)"
            },
            {
                "fieldName": "total_completion",
                "fieldNames": [
                    "total_completion"
                ],
                "type": "longSum",
                "name": "SUM(total_completion)"
            },
            {
                "fieldName": "total_certificates_issued",
                "fieldNames": [
                    "total_certificates_issued"
                ],
                "type": "longSum",
                "name": "SUM(total_certificates_issued)"
            }
        ],
        "granularity": "all",
        "postAggregations": [],
        "intervals": start_date + "/" + end_date,
        "limitSpec": {
            "type": "default",
            "limit": 50000,
            "columns": [
                {
                    "dimension": "SUM(total_enrolment)",
                    "direction": "descending"
                }
            ]
        }
    }
    jsondata = json.dumps(query_str)
    print(jsondata)
    try:
        response = requests.post('http://11.4.0.53:8082/druid/v2', headers=headers, data=jsondata)
        x = response.json()
        print(x, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        if len(x) != 0:
            for i in x:
                print(i, 'iii')
                event_list.append(i['event'])
        print(event_list)
        schema = StructType().add('content_org', StringType()).add('collection_id', StringType()).add('collection_name',
                                                                                                      StringType()) \
            .add('batch_start_date', StringType()).add('batch_end_date', StringType()).add('batch_id', StringType()) \
            .add('SUM(total_enrolment)', IntegerType()).add('SUM(total_completion)', IntegerType()).add(
            'SUM(total_certificates_issued)', IntegerType())

        base_dir = '/home/analytics/lavanya/NEW_druid_scripts/TPD_Reports/output/TPD_output/TPD_courses/' + tenant_name + '_course_wise_report.csv'

        spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "15g").appName(
            'Course_Progress_Analysis').getOrCreate()
        df_source = spark.createDataFrame(event_list, schema=schema)
        df_filter = df_source.filter(F.col('content_org') == tenant_name)
        df_filter = df_filter.filter(F.col('batch_start_date') >= '2020-04-01')
        df_snapshot = df_filter.select(F.col('collection_id').alias('course_id'),
                                       F.col('collection_name').alias('course_name'),
                                       F.col('batch_id').alias('Batch_id'),
                                       F.col('batch_start_date').alias('start_date'),
                                       F.col('batch_end_date').alias('end_date'),
                                       F.col('SUM(total_enrolment)').alias('Total_Enrollment'),
                                       F.col('SUM(total_completion)').alias('Total_Completion'),
                                       F.col('SUM(total_certificates_issued)').alias('Total_Certfication')).toPandas()

        df_snapshot.to_csv(base_dir, encoding="utf-8", index=0)
        print(df_snapshot)
        path = glob.glob(base_dir)
        tab_name = tenant_name + "_Course_wise_report"

        try:
            SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive',
                      'https://spreadsheets.google.com/feeds']

            creds = ServiceAccountCredentials.from_json_keyfile_name('/home/analytics/lavanya/client_secrete.json',
                                                                     SCOPES)

            client = gspread.authorize(creds)
            spreadsheetId = '1EGDh8mhmENKr34qMQJMgy6s6cetEaxlBFBfH7699ZrM'  # Please set spreadsheet ID.
            sheetName = tab_name  # Please set sheet name you want to put the CSV data.
            csvFile = path[0]  # Please set the filename and path of csv file.
            print('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::', path[0])

            sh = client.open_by_key(spreadsheetId)
            sh.values_update(sheetName, params={'valueInputOption': 'USER_ENTERED'},
                             body={'values': list(csv.reader(open(csvFile)))})

        except Exception as e:
            print(e)


    except Exception as e:
        print(e)


analysis_work(tenant_name)
