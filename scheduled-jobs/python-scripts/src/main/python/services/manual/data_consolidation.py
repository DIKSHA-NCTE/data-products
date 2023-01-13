from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from functools import reduce
from datetime import datetime

spark    = SparkSession.builder.appName("Engage_Funnel_Consolidation_Work").getOrCreate();
df_data  = spark.read.csv("/home/nagendra/Druid_Tibil/High_Level_Analysis/Engage_High_Level/*.csv",header=True,inferSchema=True);
df_data  = df_data.withColumn("File_Name",F.input_file_name());
df_data  = df_data.withColumn("File_Name",F.split(F.col("File_Name"),"/").getItem(6));
df_data  = df_data.withColumn("File_Name",F.regexp_replace(F.col("File_Name"),".csv"," "));
df_data  = df_data.withColumn("File_Name",F.trim(F.col("File_Name")));
df_pivot = df_data.groupBy("date").pivot("File_Name").agg(F.first("count"));
df_slice = df_pivot.select("date","DAU","Launched","New_Users","Unique_Device_Session","Total_Device_Session","Saw_User_Type","Saw_Onboarding","Onboarding_Finished","Finished_Manual_Onboarding","Saw_Location_Ask","Submitted_Location_Ask","search_button_clicked_unique_did","search_button_clicked_total_did","Landed_On_Library","Unique_Tapped_On_Textbook","Total_Tapped_On_Textbook","Unique_Tapped_Content","Total_Tapped_Content","Unique_Tapped_Qr","Total_Tapped_Qr","Scan_Initiate","Scan_Cancelled","Scan_Success","Scan_Invalid","Scan_Comming_Soon","Saw_Result","Scan_UTM_Info","Tapped_On_Play","Unique_Overall_Play","Total_Overall_Play","Content_Play_End","Content_Play_Cancel","Download_Initiate","Download_Cancelled","Download_Success").toPandas();
df_slice['date'] = df_slice['date'].apply(lambda x:x.strftime("%Y-%m-%d"));
df_slice.fillna(0.0,inplace=True);
header_list  = list(df_slice.columns);
overall_snapshot = df_slice.values.tolist();
try:
            creds = ServiceAccountCredentials.from_json_keyfile_name('/home/nagendra/*secret_json_file_path.json')

            client = gspread.authorize(creds)

            # Find a workbook by name and open the first sheet
            # Make sure you use the right name here.
            sheet1 = client.open("Diksha_App_Funnel_Engage_Auto").worksheet("Engage_Src")
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

