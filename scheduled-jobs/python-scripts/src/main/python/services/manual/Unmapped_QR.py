from pyspark.sql import SparkSession
import pandas
import sys

user_declared_state = sys.argv[1]
print(user_declared_state,'state:::::::::::::')


df_mapping = pandas.read_csv('/home/raja/Desktop/Ek_Step/Up text book/State_district.csv')
df_mapping = df_mapping.loc[df_mapping['state'] == user_declared_state]
city_list = list(df_mapping['district'])

spark = SparkSession.builder.appName('UpTextbook').getOrCreate()


df1= spark.read.csv("/home/raja/Desktop/Ek_Step/Up text book/oct-2022/oct 18/18th Oct UP Textbook - QR Code Scans 18th Oct 2022.csv",header=True)

df4=df1.filter(df1["derived_loc_district"].isin(city_list)).select("state_slug","derived_loc_district","SUM(total_count)")
#df3=df1.filter(df1["User District"].isin(city_list)).select('Date','User State','User District','User Block','User Cluster','User School Name','Total Plays','Total Unique Devices')
df4.repartition(1).write.mode('overwrite').format('csv').option('header',True)\
    .save("/home/raja/Desktop/Ek_Step/Up text book/oct-2022/oct 18/QR")