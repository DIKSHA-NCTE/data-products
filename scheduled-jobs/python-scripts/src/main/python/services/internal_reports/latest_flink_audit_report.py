import argparse
import os
import json
import gspread
import requests
import pandas as pd
import configparser
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from oauth2client.service_account import ServiceAccountCredentials


configuartion_path = os.getcwd() + "/config.ini"
print(configuartion_path)
config = configparser.ConfigParser()
config.read(configuartion_path)

parser = argparse.ArgumentParser(description="An argparse example")

parser.add_argument('start_date')
parser.add_argument('end_date')
parser.add_argument('audit_type')

args = parser.parse_args()

start_date = f"{args.start_date}"
end_date = f"{args.end_date}"
start_date = start_date + " 00:00:00"
end_date = end_date + " 00:00:00"

# Adding quotes to dates to make it query usable stuff
quoted_start_date = f"'{start_date}'"
quoted_end_date = f"'{end_date}'"
print(quoted_end_date)

uniq_tele_bucket_path = config['Bucket_Path']['unique_bucket'] + "{}-*".format(args.start_date)
denorm_tele_bucket_path = config['Bucket_Path']['denorm_bucket'] + "{}-*".format(args.start_date)
uniq_summary = config['Bucket_Path']['unique_summary'] + "{}-*".format(args.start_date)
denorm_summary = config['Bucket_Path']['denorm_summary'] + "{}-*".format(args.start_date)
raw_tele_bucket_path = config['Bucket_Path']['raw_bucket'] + "{}-*".format(args.start_date)
ingestion_tele_bucket = config['Bucket_Path']['ingestion_bucket'] + "{}-*".format(args.start_date)
raw_cluster_Druid_IP  =    config['Bucket_Path']['raw_cluster_Druid_IP']
rollup_cluster_Druid_IP = config['Bucket_Path']['rollup_cluster_Druid_IP']

spark = SparkSession.builder.appName("flink_audit").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.caseSensitive", "true")
spark.conf.set("fs.azure.sas.<diksha-container-name>.dikshaprodprivate.blob.core.windows.net", 'SAS Token')

# Schema for summary and raw data

ingestion_schema = StructType().add("params", StructType().add("msgid", StringType())) \
    .add("events", ArrayType(StructType().add("mid", StringType()).add("eid", StringType())))

raw_schema = StructType().add('eid', StringType()).add('mid', StringType())

################################  Druid Details ##########################


# Header details for POST request
headers = {
    'content-type': 'application/json',
    'Authorization': 'Bearer '
}

#################### googlesheet updation function ###########################

def gsheet_updation(my_list,columns_list,work_page,tab):
    creds = ServiceAccountCredentials.from_json_keyfile_name('/home/analytics/Raja/latest_flink/client.json') 
    client = gspread.authorize(creds)
    print(client, 'client')
    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    sheet1 = client.open(work_page).worksheet(tab)
    print(sheet1, '::::::::::::::::::::::::::::::::')
    header_course = columns_list
    length_check_one = sheet1.get_all_values()
    if (len(length_check_one) == 0):
        sheet1.insert_row(header_course, 1)
    xi = sheet1.get_all_values()
    zi = len(xi)
    for w in my_list:
        zi += 1
        sheet1.insert_row(w, zi)


def wfs_data():
    ##########################################    RAW DENORM	 #############################################################

    df_denorm = spark.read.json(path=denorm_tele_bucket_path, schema=raw_schema).select("mid").distinct()
    df_denorm = df_denorm.withColumn('Date', F.lit(args.start_date))

    df_uniq_raw2 = spark.read.json(path=uniq_tele_bucket_path, schema=raw_schema).select("mid", "eid")
    df_uniq_raw2 = df_uniq_raw2.filter(df_uniq_raw2['eid'] != 'INTERRUPT').select('mid').distinct()
    df_uniq_raw2 = df_uniq_raw2.withColumn('Date', F.lit(args.start_date))

    df_uniq2_raw_agg = df_uniq_raw2.groupBy('Date').agg(F.count("mid").alias("Input")).toPandas()
    df_denorm_agg = df_denorm.groupBy('Date').agg(F.count("mid").alias("Output")).toPandas()

    df_raw_denorm = df_uniq2_raw_agg.merge(df_denorm_agg, on=['Date'], how='inner')
    df_raw_denorm['Metric'] = 'RAW DENORM'
    df_raw_denorm = df_raw_denorm[['Date', 'Input', 'Output', 'Metric']]

    print(df_raw_denorm, 'RAW_DENORM RESULT')

    ############################################  SUMMARY DENORM  ##################################################

    df_raw_summary = spark.read.json(path=uniq_summary,encoding="UTF-8").select("mid").distinct()
    df_raw_summary = df_raw_summary.withColumn('Date', F.lit(args.start_date))

    df_denorm_summary = spark.read.json(path=denorm_summary,encoding="UTF-8").select("mid").distinct()
    df_denorm_summary = df_denorm_summary.withColumn('Date', F.lit(args.start_date))

    df_raw_summary_agg = df_raw_summary.groupBy('Date').agg(F.count("mid").alias("Input")).toPandas()
    df_denorm_summary_agg = df_denorm_summary.groupBy('Date').agg(F.count("mid").alias("Output")).toPandas()

    df_summary_denorm = df_raw_summary_agg.merge(df_denorm_summary_agg, on=['Date'], how='inner')
    df_summary_denorm['Metric'] = 'SUMMARY DENORM'
    df_summary_denorm = df_summary_denorm[['Date', 'Input', 'Output', 'Metric']]

    print(df_summary_denorm, 'SUMMARY_DENORM RESULT')
    return df_raw_denorm, df_summary_denorm, df_denorm_agg, df_denorm_summary_agg

base_dir_summary = '/home/analytics/Raja/latest_flink/flink_audit_summary'+args.end_date+'.csv'

############# function of telemetry_events_syncts and telemetry_rollup_syncts ##################################

def summary_audit():
    wfs_result = wfs_data()
    df_summary_denorm=wfs_result[3]
    # Number of devices who reached on onboarding summary_events
    summary_events_query_str = '''SELECT COUNT(*)  AS "Output" FROM "druid"."summary-events" WHERE  "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date
    summary_events_data = {"query": summary_events_query_str}
    summary_events_jsondata = json.dumps(summary_events_data)

    # Number of devices who reached on onboarding summary_rollup_syncts
    summary_rollup_query_str = '''SELECT SUM(total_count)  AS "Output" FROM "druid"."summary-rollup-syncts" WHERE  "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date
    summary_rollup_data = {"query": summary_rollup_query_str}
    summary_rollup_jsondata = json.dumps(summary_rollup_data)

    # Fetching data from Druid using POST request
    try:
        summary_events_response = requests.post(raw_cluster_Druid_IP, headers=headers, data=summary_events_jsondata)
        summary_rollup_response = requests.post(rollup_cluster_Druid_IP, headers=headers, data=summary_rollup_jsondata)


        x_summary_events = summary_events_response.json()
        x_summary_rollup = summary_rollup_response.json()

        if ((len(x_summary_events) != 0) & (len(x_summary_rollup) != 0)):

            df_summary_events = pd.DataFrame.from_dict(x_summary_events)
            df_summary_rollup = pd.DataFrame.from_dict(x_summary_rollup)


            df_summary_events['Date'] = args.start_date
            df_summary_rollup['Date'] = args.start_date


            df_summary_denorm = df_summary_denorm[['Date', 'Output']]
            df_summary_denorm.columns = ['Date', 'Input']

            df_join_summary_events = df_summary_events.merge(df_summary_denorm, on=['Date'], how='inner')
            df_join_summary_rollup = df_summary_rollup.merge(df_summary_denorm, on=['Date'], how='inner')


            df_join_summary_events['Metric'] = 'SUMMARY DRUID'
            df_join_summary_events = df_join_summary_events[['Date', 'Input', 'Output', 'Metric']]
            df_join_summary_rollup['Metric'] = 'ROLLUP SUMMARY DRUID'
            df_join_summary_rollup = df_join_summary_rollup[['Date', 'Input', 'Output', 'Metric']]

            li = [df_join_summary_events, df_join_summary_rollup]
            frame = pd.concat(li, axis=0, ignore_index=True)

            frame = frame.pivot("Date", "Metric", ['Input', 'Output'])
            frame = frame[
                [('Input', 'SUMMARY DRUID'), ('Output', 'SUMMARY DRUID'),('Input', 'ROLLUP SUMMARY DRUID'), ('Output', 'ROLLUP SUMMARY DRUID')]]
            frame.columns = [ 'SUMMARY DRUID-Input', 'SUMMARY DRUID-Output','ROLLUP SUMMARY DRUID-Input','ROLLUP SUMMARY DRUID-Output']
            frame.to_csv(base_dir)
            df=pd.read_csv(base_dir)

            my_list = df.values.tolist()
            columns_list = df.columns.tolist()
            print(my_list,'mylist:::::::::::::::::::::::::::')
            print(columns_list,'column_list:::::::::::::::::::::::::::::::::')

            gsheet_updation(my_list, columns_list, work_page='Internal Sheet FLINK_AUDIT', tab='summary_audit_result')

            return
    except Exception as e:
        print(e)

base_dir_raw = '/home/analytics/Raja/latest_flink/flink_audit_raw'+args.end_date+'.csv'



def raw_audit():
    wfs_result = wfs_data()
    df_denorm = wfs_result[2]

    # Number of devices who reached on onboarding telemetry_events_syncts
    events_query_str = '''SELECT COUNT(*)  AS "Output" FROM "druid"."telemetry-events-syncts" WHERE  "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date
    events_data = {"query": events_query_str}
    events_jsondata = json.dumps(events_data)

    # Number of devices who reached on onboarding telemetry_rollup_syncts
    rollup_query_str = '''SELECT SUM(total_count)  AS "Output" FROM "druid"."telemetry-rollup-syncts" WHERE  "__time">=''' + quoted_start_date + '''AND "__time"<''' + quoted_end_date
    rollup_data = {"query": rollup_query_str}
    rollup_jsondata = json.dumps(rollup_data)


    # Fetching data from Druid using POST request
    try:
        events_response = requests.post(raw_cluster_Druid_IP, headers=headers, data=events_jsondata)
        rollup_response = requests.post(rollup_cluster_Druid_IP, headers=headers, data=rollup_jsondata)

        x_events = events_response.json()
        x_rollup = rollup_response.json()

        if ((len(x_events) != 0) & ((len(x_rollup) != 0))):
            df_events = pd.DataFrame(x_events)
            df_rollup = pd.DataFrame(x_rollup)

            df_events['Date'] = args.start_date
            df_rollup['Date'] = args.start_date

            df_denorm = df_denorm[['Date', 'Output']]
            df_denorm.columns = ['Date', 'Input']

            df_join_events = df_events.merge(df_denorm, on=['Date'], how='inner')
            df_join_rollup = df_rollup.merge(df_denorm, on=['Date'], how='inner')

            df_join_events['Metric'] = 'RAW DRUID'
            df_join_events = df_join_events[['Date', 'Input', 'Output', 'Metric']]
            df_join_rollup['Metric'] = 'ROLLUP RAW DRUID'
            df_join_rollup = df_join_rollup[['Date', 'Input', 'Output', 'Metric']]

            li = [df_join_events,df_join_rollup]
            frame = pd.concat(li, axis=0, ignore_index=True)

            frame = frame.pivot("Date", "Metric", ['Input', 'Output'])
            frame = frame[
                [('Input', 'RAW DRUID'), ('Output', 'RAW DRUID'), ('Input', 'ROLLUP RAW DRUID'),('Output', 'ROLLUP RAW DRUID')]]
            frame.columns = ['RAW DRUID-Input', 'RAW DRUID-Output','ROLLUP RAW DRUID-Input', 'ROLLUP RAW DRUID-Output']
            frame.to_csv(base_dir)
            df = pd.read_csv(base_dir)

            my_list = df.values.tolist()
            columns_list = df.columns.tolist()
            print(my_list, 'mylist:::::::::::::::::::::::::::')
            print(columns_list, 'column_list:::::::::::::::::::::::::::::::::')

            #gsheet_updation(my_list, columns_list, work_page='FLINK_AUDIT_Report', tab='raw_audit_result')
            gsheet_updation(my_list, columns_list, work_page='Internal Sheet FLINK_AUDIT', tab='raw_audit_result')

            return
    except Exception as e:
        print(e)

base_dir = '/home/analytics/Raja/latest_flink/flink_audit_blob'+args.end_date+'.csv'


def raw_unique_data():

    ############################## EXTRACTOR ######################################################

    df_ingestion = spark.read.json(ingestion_tele_bucket, schema=ingestion_schema)
    df_ingestion = df_ingestion.withColumn('Date', F.lit(args.start_date))
    df_ingestion = df_ingestion.filter(df_ingestion['params']['msgid'].isNotNull()).distinct()
    df_ingestion = df_ingestion.withColumn('explod_event', F.explode(F.col('events')))
    df_ingestion = df_ingestion.filter(df_ingestion['explod_event']['eid'] != 'LOG')

    df_raw = spark.read.json(raw_tele_bucket_path, schema=raw_schema).select("mid", "eid")
    df_raw = df_raw.filter((df_raw['eid'] != 'LOG') & (df_raw['eid']!= 'SEARCH') & (df_raw['eid']!= 'METRICS')).select('mid').distinct()
    df_raw = df_raw.withColumn('Date', F.lit(args.start_date))

    df_ingestion_agg = df_ingestion.groupBy('Date').agg(F.count("explod_event.mid").alias("Input")).toPandas()
    df_raw_agg = df_raw.groupBy('Date').agg(F.count("mid").alias("Output")).toPandas()

    df_extractor_merge = df_raw_agg.merge(df_ingestion_agg, on=['Date'], how='inner')
    df_extractor_merge['Metric'] = 'EXTRACTOR'
    df_extractor_merge = df_extractor_merge[['Date', 'Input', 'Output', 'Metric']]

    print(df_extractor_merge,'EXTRACTOR RESULT')
    ##########################################  PREPROCESSOR  ##################################################################

    df_uniq_raw = spark.read.json(path=uniq_tele_bucket_path, schema=raw_schema).select("mid", "eid")
    df_uniq_raw = df_uniq_raw.filter(df_uniq_raw['eid'] != 'SHARE_ITEM').select('mid').distinct()
    df_uniq_raw = df_uniq_raw.withColumn('Date', F.lit(args.start_date))

    df_raw2 = spark.read.json(raw_tele_bucket_path, schema=raw_schema).select("mid", "eid")
    df_raw2 = df_raw2.filter((df_raw2['eid'] != 'LOG') & (df_raw2['eid'] != 'ERROR')).select('mid').distinct()
    df_raw2 = df_raw2.withColumn('Date', F.lit(args.start_date))

    df_raw2_agg = df_raw2.groupBy('Date').agg(F.count("mid").alias("Input")).toPandas()
    df_uniq_raw_agg = df_uniq_raw.groupBy('Date').agg(F.count("mid").alias("Output")).toPandas()

    df_preprocessor_merge = df_raw2_agg.merge(df_uniq_raw_agg, on=['Date'], how='inner')
    df_preprocessor_merge['Metric'] = 'PREPROCESSOR'
    df_preprocessor_merge = df_preprocessor_merge[['Date', 'Input', 'Output', 'Metric']]

    print(df_preprocessor_merge,'PREPROCESSPOR RESULT')
    wfs_result = wfs_data()
    df_raw_denorm = wfs_result[0]
    df_summary_denorm = wfs_result[1]

    li = [df_extractor_merge, df_preprocessor_merge, df_raw_denorm, df_summary_denorm]
    frame = pd.concat(li, axis=0, ignore_index=True)

    frame = frame.pivot("Date", "Metric", ['Input', 'Output'])
    frame = frame[
        [('Input', 'EXTRACTOR'), ('Output', 'EXTRACTOR'), ('Input', 'PREPROCESSOR'), ('Output', 'PREPROCESSOR'),
         ('Input', 'RAW DENORM'), ('Output', 'RAW DENORM'), ('Input', 'SUMMARY DENORM'), ('Output', 'SUMMARY DENORM')]]
    frame.columns = ['EXTRACTOR-Input', 'EXTRACTOR-Output', 'PREPROCESSOR-Input', 'PREPROCESSOR-Output',
                     'RAW DENORM-Input', 'RAW DENORM-Output', 'SUMMARY DENORM-Input', 'SUMMARY DENORM-Output']
    frame.to_csv(base_dir)
    df = pd.read_csv(base_dir)
    my_list =df.values.tolist()
    columns_list = ['Date', 'EXTRACTOR-Input', 'EXTRACTOR-Output', 'PREPROCESSOR-Input', 'PREPROCESSOR-Output',
                     'RAW DENORM-Input', 'RAW DENORM-Output', 'SUMMARY DENORM-Input', 'SUMMARY DENORM-Output']
    print(my_list, 'mylist:::::::::::::::::::::::::::')
    print(columns_list, 'column_list:::::::::::::::::::::::::::::::::')

    gsheet_updation(my_list,columns_list,work_page='Internal Sheet FLINK_AUDIT',tab='raw_unique_blob_result')


if args.audit_type=='raw_unique':
    raw_unique_data()


elif args.audit_type=='summary_audit':
     summary_audit()

elif args.audit_type=='raw_audit':
     raw_audit()

else:
    pass





