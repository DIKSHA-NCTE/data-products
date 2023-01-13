from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys

date = sys.argv[1]
output_dir_path = "home/analytics/Raja/outputs/dftu_outputs/DFTU_5.0.1044/New_User/" + date + "/"

spark = SparkSession.builder.appName("Detailed_FTU_Analysis").getOrCreate()

# Specify the schema as follows,causes to decrease the load time
schema = StructType().add("actor", StructType().add("type", StringType()).add("id", StringType())) \
    .add("context", StructType().add("did", StringType()).add("env", StringType()) \
    .add("pdata", StructType().add("id", StringType()).add("pid", StringType()).add("ver", StringType()))).add("eid", StringType()) \
    .add("edata", StructType().add("pageid",StringType()).add("id", StringType()).add("mode", StringType()).add("subtype", StringType()).add("type", StringType())) \
    .add("object", StructType().add("type", StringType()).add("id", StringType()))


# save unique did count to csv file
def saveToCsv(df_uni_dev, matrix_name):
    output_dir = output_dir_path + matrix_name
    # output file directory
    df_uni_dev.repartition(1).write.format("csv").option("header", True).mode("overwrite").save(output_dir)


spark = SparkSession.builder.appName("DFTU_Analysis").getOrCreate()

path = "/home/analytics/Raja/dftu/2022-11-29/"+date+"/*.json.gz"

# input file directory
df = spark.read.json(path, schema=schema)

# Schema of the dataset
df.printSchema()

# Global filter for extarcting devices on the app with version =5.0
df_global = df.filter( (df['context']['pdata']['ver'] == '5.0.1044') & (df['context']['pdata']['id'] == 'prod.diksha.app')\
     & (df['eid'].isin(['INTERACT','START','END','IMPRESSION'])))

# Deriving date column in dataset
df_global = df_global.withColumn("date", F.lit(date))

# Devices Fired Language Selection
df_new_user = df_global.filter(
    (df_global['context']['pdata']['pid'] == 'sunbird.app') & (df_global['eid'] == 'IMPRESSION') \
    & (df_global['edata']['type'] == 'view') & (df_global['context']['env'] == 'onboarding') & (
                df_global['edata']['pageid'] == 'onboarding-language-setting') \
    & (df_global['context']['pdata']['id'] == 'prod.diksha.app')).select(F.col("context.did").alias("did"), "date")
# Extraction of devices where users tapped scan
df_tapped_scan = df_global.filter(
    (df_global['context']['pdata']['pid'] == 'sunbird.app') & (df_global['eid'] == 'INTERACT') \
    & (df_global['edata']['pageid'] == 'profile-settings') & (df_global['edata']['type'] == 'TOUCH') \
    & (df_global['edata']['subtype'] == 'qr-code-scanner-clicked') & (df_global['context']['env'] == 'onboarding') \
    & (df_global['context']['pdata']['id'] == 'prod.diksha.app')) \
    .select(F.col("context.did").alias("did"), "date")

# Extraction of  devices where users saw result
df_saw_result = df_global.filter(
    (df_global['context']['pdata']['pid'] == 'sunbird.app') & (df_global['eid'] == 'IMPRESSION') \
    & (df_global['edata']['type'] == 'view') & (df_global['edata']['pageid'] == 'dial-code-scan-result') \
    & (df_global['context']['pdata']['id'] == 'prod.diksha.app')) \
    .select(F.col("context.did").alias("did"), "date")

# Extraction of did play
df_play = df_global.filter((df_global['eid'] == 'START') & (df_global['edata']['type'] == 'content') & (df_global['context']['env'] == 'contentplayer')).select(F.col("context.did").alias("did"), "date")

# Extraction of  did play
df_play_new = df_global.filter((df_global['eid'] == 'START') & (df_global['edata']['type'] == 'content') \
                           & ((df_global['context']['env'] == 'contentplayer') | (df_global['context']['env'] == 'ContentPlayer'))).select(F.col("context.did").alias("did"), "date")
# Extraction of devices where users tapped qr icon
df_tapped_qr_icon = df_global.filter(
    (df_global['context']['pdata']['pid'] == 'sunbird.app') & (df_global['eid'] == 'INTERACT') & (df_global['edata']['type'] == 'TOUCH') \
    & (df_global['edata']['subtype'] == 'tab-clicked') & (df_global['edata']['pageid'] == 'qr-code-scanner')).select(
    F.col("context.did").alias("did"), "date")

# Extraction of did where users tapped textbook
df_tapped_textbook = df_global.filter((df_global['context']['pdata']['pid'] == 'sunbird.app') & (df_global['eid'] == 'INTERACT') & (df_global['edata']['type'] == 'TOUCH') \
    & (df_global['edata']['subtype'] == 'content-clicked') & (df_global['object']['type'] == 'Digital Textbook') \
    & (df_global['context']['env'] == 'home') & (df_global['edata']['pageid'] == 'library')).select(
    F.col("context.did").alias("did"), "date")


def New_Users():
    df_uni_dev = df_new_user.groupBy("date").agg(F.countDistinct("did"))
    # df_uni_dev.show();

    saveToCsv(df_uni_dev, sys._getframe().f_code.co_name)


def Saw_User_Type():
    df_local = df_global.filter(
        (df_global['context']['pdata']['pid'] == 'sunbird.app') & (df_global['eid'] == 'IMPRESSION') \
        & (df_global['edata']['type'] == 'view') & (df_global['context']['env'] == 'onboarding') \
        & (df_global['edata']['pageid'] == 'user-type-selection') & (
                    df_global['context']['pdata']['id'] == 'prod.diksha.app')) \
        .select(F.col("context.did").alias("did"), "date")

    df_slice = df_new_user.select("did").distinct()

    df_merge = df_local.join(df_slice, ["did"], how='inner')

    df_uni_dev = df_merge.groupBy("date").agg(F.countDistinct("did"))

    saveToCsv(df_uni_dev, sys._getframe().f_code.co_name)



def Saw_Onboarding():
    df_local = df_global.filter(
        (df_global['context']['pdata']['pid'] == 'sunbird.app') & (df_global['eid'] == 'IMPRESSION') \
        & (df_global['edata']['type'] == 'view') & (df_global['context']['env'] == 'onboarding') \
        & (df_global['edata']['pageid'] == 'profile-settings') & (
                    df_global['context']['pdata']['id'] == 'prod.diksha.app')) \
        .select(F.col("context.did").alias("did"), "date")

    df_slice = df_new_user.select("did").distinct()

    df_merge = df_local.join(df_slice, ["did"], how='inner')

    df_uni_dev = df_merge.groupBy("date").agg(F.countDistinct("did"))

    saveToCsv(df_uni_dev, sys._getframe().f_code.co_name)

def Landed_On_Library():
    df_local = df_global.filter(
        (df_global['context']['pdata']['pid'] == 'sunbird.app') & (df_global['eid'] == 'IMPRESSION') \
        & (df_global['edata']['type'] == 'view') & (df_global['context']['env'] == 'home') & (
            df_global['edata']['pageid'].isin(['library', 'resources']))) \
        .select(F.col("context.did").alias("did"), "date")

    df_new_user_slice = df_new_user.select("did").distinct()

    df_merge = df_local.join(df_new_user_slice, ["did"], how='inner')

    df_uni_dev = df_merge.groupBy("date").agg(F.countDistinct("did"))

    saveToCsv(df_uni_dev, sys._getframe().f_code.co_name)

def Tapped_QR_Icon():
    df_local = df_global.filter(
        (df_global['context']['pdata']['pid'] == 'sunbird.app') & (df_global['eid'] == 'INTERACT') & (
                    df_global['edata']['type'] == 'TOUCH') \
        & (df_global['edata']['subtype'] == 'tab-clicked') & (
                    df_global['edata']['pageid'] == 'qr-code-scanner')).select(F.col("context.did").alias("did"),
                                                                               "date")

    df_new_user_slice = df_new_user.select("did").distinct()

    df_merge = df_local.join(df_new_user_slice, ["did"], how='inner')

    df_uni_dev = df_merge.groupBy("date").agg(F.countDistinct("did"))

    saveToCsv(df_uni_dev, sys._getframe().f_code.co_name)

def Tapped_Textbook():

    df_new_user_slice = df_new_user.select("did").distinct()

    df_merge = df_tapped_textbook.join(df_new_user_slice, ["did"], how='inner')

    df_uni_dev = df_merge.groupBy("date").agg(F.countDistinct("did"))

    saveToCsv(df_uni_dev, sys._getframe().f_code.co_name)


def Onboarding_Complete():

    df_local = df_global.filter((df_global['context']['pdata']['pid'] == 'sunbird.app') & (df_global['eid'] == 'INTERACT') \
        & (df_global['edata']['type'] == 'OTHER') & (df_global['edata']['subtype'] == 'profile-attribute-population') \
        & (df_global['context']['pdata']['id'] == 'prod.diksha.app')) \
        .select(F.col("context.did").alias("did"), "date")

    df_slice = df_new_user.select("did").distinct()

    df_merge = df_local.join(df_slice, ["did"], how='inner')

    df_uni_dev = df_merge.groupBy("date").agg(F.countDistinct("did"))

    saveToCsv(df_uni_dev, sys._getframe().f_code.co_name)


def Overall_Play():

    df_new_user_slice = df_new_user.select("did").distinct()

    df_merge = df_play.join(df_new_user_slice, ["did"], how='inner')

    df_uni_dev = df_merge.groupBy("date").agg(F.countDistinct("did"))

    saveToCsv(df_uni_dev, sys._getframe().f_code.co_name)

def Overall_Play_new():

    df_new_user_slice = df_new_user.select("did").distinct()

    df_merge = df_play_new.join(df_new_user_slice, ["did"], how='inner')

    df_uni_dev = df_merge.groupBy("date").agg(F.countDistinct("did"))

    saveToCsv(df_uni_dev, sys._getframe().f_code.co_name)

New_Users()
Overall_Play_new()
Saw_User_Type()
Saw_Onboarding()
Landed_On_Library()
Tapped_QR_Icon()
Tapped_Textbook()
Onboarding_Complete()
#Overall_Play()

