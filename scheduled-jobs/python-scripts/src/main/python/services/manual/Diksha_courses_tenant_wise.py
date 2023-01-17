import csv
import glob
import json
import numpy as np
import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import timedelta, date
from azure.storage.blob import BlockBlobService
from pyspark.sql.types import *

date_work = date.today()
Date=date_work.strftime("%Y-%m-%d")
print(date_work)
end_dates = date_work
start_dates = date_work - timedelta(days=1)
start_date = start_dates.strftime("%Y-%m-%d") + "T00:00:00+00:00"
end_date = end_dates.strftime("%Y-%m-%d") + "T00:00:00+00:00"

#base_dir = '/home/ramya/NEW_druid_scripts/Diksha_courses/output/batch_wise/All_Tenant_Overall_report_' + Date + '.csv'
base_dir = '/home/analytics/lavanya/NEW_druid_scripts/Diksha_courses/output/batch_wise/All_Tenant_Overall_report_' + Date + '.csv'

def Upload_File():
    my_list_files = glob.glob(base_dir)
    blob_file = []
    # print(my_list_files);
    for i in my_list_files:
        x = i.split('/')
        y = x[8:]

        z = '/'.join(y)
        blob_file.append(z)

    block_blob_service = BlockBlobService(account_name='',
                                          sas_token="")

    for i in my_list_files:
        for j in blob_file:
            if (j in i):
                block_blob_service.create_blob_from_path('tdp-reports', 'All_Tenant_report/' + j, i)

                print("File Uploaded", i)

event_list=[]

def analysis_work():
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
    "batch_end_date",
    "has_certificate"
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
  "intervals": start_date+"/"+end_date,
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
        if len(x) != 0:
            for i in x:
                event_list.append(i['event'])
        schema = StructType().add('content_org', StringType()).add('collection_id', StringType()).add('collection_name',StringType()) \
            .add('batch_start_date', StringType()).add('batch_end_date', StringType()).add('batch_id',StringType()).add('has_certificate', StringType()) \
            .add('SUM(total_enrolment)', IntegerType()).add('SUM(total_completion)', IntegerType()).add('SUM(total_certificates_issued)', IntegerType())
        spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "15g").appName('Course_Progress_Analysis').getOrCreate()
        df_source = spark.createDataFrame(event_list, schema=schema)
        df_snapshot = df_source.select(F.col('content_org').alias('Published by'), 'collection_id', 'collection_name','batch_id', 'batch_start_date','batch_end_date', 'has_certificate',
                                   F.col('SUM(total_enrolment)').alias('Total_Enrollment'),
                                   F.col('SUM(total_completion)').alias('Total_Completion'),
                                   F.col('SUM(total_certificates_issued)').alias('Total_Certifications')).toPandas()

        df_snapshot.info()

        df_snapshot['Cert_Lag'] = np.where(df_snapshot['has_certificate'] == 'Y',df_snapshot['Total_Completion'] - df_snapshot['Total_Certifications'], 0)
        df_snapshot = df_snapshot.groupby('Published by', as_index=False).agg({'Total_Enrollment': 'sum', 'Total_Completion': 'sum', 'Total_Certifications': 'sum', 'Cert_Lag': 'sum'})
        print(df_snapshot)
        daily_snapshot = df_snapshot[['Published by', 'Total_Enrollment', 'Total_Completion', 'Total_Certifications', 'Cert_Lag']]

        daily_snapshot.columns = ['tenant_name', 'Total_Enrollment', 'Total_Completion', 'Total_Certifications', 'Cert_Lag']

        daily_snapshot.to_csv(base_dir, encoding="utf-8", index=0)
        print(df_snapshot)

    except Exception as e:
        print(e)

    path = glob.glob(base_dir)
    tab_name = 'Overall_report-Druid'

    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive',
                  'https://spreadsheets.google.com/feeds']

        creds = ServiceAccountCredentials.from_json_keyfile_name('/home/analytics/lavanya/NEW_druid_scripts/client_secrete.json', SCOPES)

        client = gspread.authorize(creds)
        spreadsheetId = '1ay9x0Qx_6gTGRTcfqLX0H-5YvQfKJcbWjqRW72msas4'  # Please set spreadsheet ID.
        #spreadsheetId = '1YAVANAmis8yudoq-rsnpOlnbyAr2qN5i2YyfJfMSLs0'
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


analysis_work()
Upload_File()
