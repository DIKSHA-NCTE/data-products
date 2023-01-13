from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("read").getOrCreate()

df = spark.read.csv("/home/raja/Desktop/Ekstep/cassandra/user/sunbird.user_202211020720.csv", header=True)

df_filter = df.filter(df['profileusertype'].isNull()).select('userid', 'usertype', 'createddate')

df_filter.repartition(1).write.mode('overwrite').format('csv').option('header', True).save("/home/raja/Desktop/Ekstep/cassandra/user/output")
