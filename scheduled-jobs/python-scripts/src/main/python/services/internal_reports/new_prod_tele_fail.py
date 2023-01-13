import json
import shutil
from pathlib import Path

from datetime import datetime, timedelta
from azure.storage.blob import BlockBlobService
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn

#pre_date = sys.argv[1]

pre_date = datetime.now() - timedelta(days=1)
pre_date.strftime('%Y-%m-%d')

def generate_azure_path(container_name, account, prefix, date_):
    return 'wasbs://{}@{}.blob.core.windows.net/{}/{}-*'.format(container_name, account, prefix,
                                                                date_.strftime('%Y-%m-%d'))


def spark_init(spark_app_, container_name, account_name_, account_key_):
    spark_ = SparkSession.builder.appName(spark_app_).master("local[*]").getOrCreate()
    spark_.conf.set('fs.azure.sas.{}.{}.blob.core.windows.net'.format(container_name, account_name_), account_key_)
    return spark_


def post_data_to_blob(result_loc_, env):
    """
    write a local file to blob storage.
    :param env: environment ['prod', 'diksha_preprod_', 'diksha_staging_']
    :param result_loc_: pathlib.Path object to read data from
    :return: None
    """
    try:
        account_name = access_keys[env]['write_account']
        account_key = access_keys[env]['write_sas']
        block_blob_service = BlockBlobService(account_name=account_name, sas_token=account_key)
        container_name = 'tdp-reports'
        block_blob_service.create_blob_from_path(
            container_name=container_name,
            blob_name=str(result_loc_).split('/', 4)[-1],
            file_path=str(result_loc_)
        )
    except Exception:
        raise Exception('Failed to post to blob!')


def process_failed_events(file_path, env, pre_date):
    file_store_path = Path(file_path)
    #analysis_date = datetime.strptime(pre_date,'%Y-%m-%d')
    analysis_date = pre_date
    file_store_path.mkdir(exist_ok=True)
    file_store_path.joinpath(analysis_date.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
    file_store_path.joinpath(analysis_date.strftime('%Y-%m-%d'), 'sample').mkdir(exist_ok=True)
    #file_store_path.joinpath(pre_data).mkdir(exist_ok=True)
    #file_store_path.joinpath(pre_data, 'sample').mkdir(exist_ok=True) 
    container = 'telemetry-data-store'
    account_name = access_keys[env]['read_account']
    account_key = access_keys[env]['read_sas']
    prefix1 = 'failed'
    spark_app = 'failed_events'
    spark = spark_init(spark_app, container, account_name, account_key)
    path1 = generate_azure_path(container, account_name, prefix1, analysis_date)
    data = spark.read.json(path1)
    df = data.filter(
        fn.col('metadata.src').isin(['PipelinePreprocessor'])
    ).groupby(
        fn.col('context.pdata.pid').alias('context_pdata_pid'),
        fn.col('eid'),
        fn.col('context.pdata.ver').alias('context_pdata_ver'),
        fn.col('metadata.validation_error')
    ).count().withColumn(
        'Date', fn.lit(analysis_date.strftime('%Y-%m-%d'))
    ).select(
        fn.col('Date'),
        fn.col('context_pdata_pid'),
        fn.col('eid'),
        fn.col('context_pdata_ver'),
        fn.col('validation_error'),
        fn.col('count')
    ).toPandas()
    df.sort_values('count', ascending=False, inplace=True)
    df.to_csv(file_store_path.joinpath(analysis_date.strftime('%Y-%m-%d'), 'summary.csv'), index=False)
    post_data_to_blob(file_store_path.joinpath(analysis_date.strftime('%Y-%m-%d'), 'summary.csv'), env)
    if env == 'diksha_prod_':
        df = df[:20]
    for ind, row in df.iterrows():
        query = data
        if row['context_pdata_pid'] is None:
            pid = 'Null'
            query = query.filter(
                fn.col('context.pdata.pid').isNull()
            )
        else:
            pid = row['context_pdata_pid']
            query = query.filter(
                fn.col('context.pdata.pid').isin([pid])
            )
        if row['eid'] is None:
            eid = 'Null'
            query = query.filter(
                fn.col('eid').isNull()
            )
        else:
            eid = row['eid']
            query = query.filter(
                fn.col('eid').isin([eid])
            )
        if row['context_pdata_ver'] is None:
            ver = 'Null'
            query = query.filter(
                fn.col('context.pdata.ver').isNull()
            )
        else:
            ver = row['context_pdata_ver']
            query = query.filter(
                fn.col('context.pdata.ver').isin([ver])
            )
        if row['validation_error'] is None:
            error = 'Null'
            query = query.filter(
                fn.col('metadata.validation_error').isNull()
            )
        else:
            error = row['validation_error']
            query = query.filter(
                fn.col('metadata.validation_error').isin([error])
            )
        file_store_path.joinpath(analysis_date.strftime('%Y-%m-%d'), 'sample', pid).mkdir(exist_ok=True)
        file_store_path.joinpath(analysis_date.strftime('%Y-%m-%d'), 'sample', pid, eid).mkdir(exist_ok=True)
        file_store_path.joinpath(analysis_date.strftime('%Y-%m-%d'), 'sample', pid, eid, ver).mkdir(exist_ok=True)
        with open(
                str(
                    file_store_path.joinpath(
                        analysis_date.strftime('%Y-%m-%d'),
                        'sample',
                        pid,
                        eid,
                        ver,
                        error.replace('/', '_') + '.json'
                    )
                ),
                'w'
        ) as f:
            json.dump(json.loads(query.toJSON().first()), f)
    spark.stop()
    shutil.make_archive(
        str(file_store_path.joinpath(analysis_date.strftime('%Y-%m-%d'), 'sample')),'zip',
        str(file_store_path.joinpath(analysis_date.strftime('%Y-%m-%d'), 'sample'))
    )
    post_data_to_blob(file_store_path.joinpath(analysis_date.strftime('%Y-%m-%d'), 'sample.zip'), env)
   # shutil.rmtree(str(file_store_path))


access_keys = {
    'prod': {
        'read_account': "",
        'read_sas': "",
        'write_account': "",
        'write_sas': " "
    },
    'preprod': {
        'read_account': "",
        'read_sas': "",
        'write_account': "",
        'write_sas': ""
    },
    'staging': {
        'read_account': "",
        'read_sas': "",
        'write_account': "",
        'write_sas': ""
    }
}

#process_failed_events('/home/analytics/lavanya/telemetry_failure/output', 'staging', pre_date)
#process_failed_events('/home/analytics/lavanya/telemetry_failure/preprod_output1', 'preprod', pre_date)
process_failed_events('/home/analytics/lavanya/telemetry_failure/prod_output1', 'prod', pre_date)

