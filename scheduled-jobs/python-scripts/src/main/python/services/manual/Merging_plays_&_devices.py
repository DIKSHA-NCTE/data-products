from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Merging").getOrCreate()

df_devices = spark.read.csv('/home/raja/Desktop/Ek_Step/Up text book/oct 6/Recent UP Textbook Data-4th Oct 2022 - TB Devices 4th Oct 2022.csv',header=True)
df_plays = spark.read.csv("/home/raja/Desktop/Ek_Step/Up text book/oct 6/Recent UP Textbook Data-4th Oct 2022 - TB Plays 4th Oct 2022.csv",header=True)

df_plays.printSchema()
df_devices.printSchema()

op_df = df_devices.join(df_plays,["collection_channel_slug","object_rollup_l1","collection_name","collection_board","collection_medium","collection_gradelevel","collection_subject"],how='inner')
op_df.write.mode('Overwrite').option('header',True).format('CSV')\
    .save('/home/raja/Desktop/Ek_Step/Up text book/oct 6/result')


