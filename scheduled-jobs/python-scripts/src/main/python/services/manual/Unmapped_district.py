from pyspark.sql import SparkSession
import pandas
import sys

user_declared_state = sys.argv[1]
print(user_declared_state,'state:::::::::::::')


df_mapping = pandas.read_csv('/home/raja/Desktop/Ek_Step/Up text book/State_district.csv')
df_mapping = df_mapping.loc[df_mapping['state'] == user_declared_state]
city_list = list(df_mapping['district'])

spark = SparkSession.builder.appName('UpTextbook').getOrCreate()


df1= spark.read.csv("/home/raja/Desktop/Ek_Step/Up text book/oct 6/Recent UP Textbook Data-4th Oct 2022 - District 4th Oct 2022.csv",header=True)

df4=df1.filter(df1["User District"].isin(city_list)).select('Date','User State','User District','User Block','User Cluster','User School Name','Total Plays','Total Unique Devices')
#df3=df1.filter(df1["User District"].isin(city_list)).select('Date','User State','User District','User Block','User Cluster','User School Name','Total Plays','Total Unique Devices')
df4.repartition(1).write.mode('overwrite').format('csv').option('header',True).save("/home/raja/Desktop/Ek_Step/Up text book/oct 6/district")