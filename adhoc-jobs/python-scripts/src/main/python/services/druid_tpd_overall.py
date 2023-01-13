import csv
import glob
import json
import sys
from pyspark.sql.types import *
import requests
from pyspark.sql import functions as F
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pyspark.sql import SparkSession
from datetime import timedelta, date

date_work = date.today()
print(date_work)
end_dates = date_work
start_dates = date_work - timedelta(days=1)
start_date = start_dates.strftime("%Y-%m-%d") + "T00:00:00+00:00"
end_date = end_dates.strftime("%Y-%m-%d") + "T00:00:00+00:00"

event_list = []
####################################################

tenant_name = sys.argv[1]


def analysis_work(tenant_name):
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer '
    }
    query_str = {
  "queryType": "topN",
  "dataSource": "collection-summary-snapshot",
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
  "filter": {
    "type": "bound",
    "dimension": "batch_start_date",
    "lower": "2020-04-01",
    "lowerStrict": 'false',
    "upper": 'null',
    "upperStrict": 'false',
    "alphaNumeric": 'false'
  },
  "threshold": 50000,
  "metric": "SUM(total_enrolment)",
  "dimension": "content_org"
}
    jsondata = json.dumps(query_str)
    print(jsondata)
    try:
        response = requests.post('http://11.4.0.53:8082/druid/v2', headers=headers, data=jsondata)
        x = response.json()
        for i in x:
            for j in i['result']:
                print(j,'1111111111111')
                event_list.append(j)

        schema = StructType().add('content_org', StringType()).add('SUM(total_enrolment)', IntegerType()).add('SUM(total_completion)', IntegerType()).add('SUM(total_certificates_issued)', IntegerType())

        base_dir = '/home/analytics/lavanya/NEW_druid_scripts/TPD_Reports/output/TPD_output/TPD_overall/' + tenant_name + '_overall_report.csv'

        spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "15g").appName('TPD_Courses').getOrCreate()
        df_source = spark.createDataFrame(event_list, schema=schema)
        
        df_filter=df_source.filter(F.col('content_org')==tenant_name)
        df_snapshot=df_filter.select(F.col('content_org').alias('Published_By'),F.col('SUM(total_enrolment)').alias('Total_Enrollment'),F.col('SUM(total_completion)').alias('Total_Completion'),F.col('SUM(total_certificates_issued)').alias('Total_Certfication')).toPandas()

        df_snapshot.to_csv(base_dir, encoding="utf-8", index=0)
        print(df_snapshot)

    except Exception as e:
        print(e)

    path = glob.glob(base_dir)
    tab_name = tenant_name + "_Overall"

    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive',
                  'https://spreadsheets.google.com/feeds']

        creds = ServiceAccountCredentials.from_json_keyfile_name('/home/analytics/lavanya/client_secrete.json', SCOPES)

        client = gspread.authorize(creds)
        spreadsheetId = '1EGDh8mhmENKr34qMQJMgy6s6cetEaxlBFBfH7699ZrM'  # Please set spreadsheet ID.
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


analysis_work(tenant_name)

