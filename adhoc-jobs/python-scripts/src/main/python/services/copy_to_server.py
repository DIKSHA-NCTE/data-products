from azure.storage.blob import BlockBlobService
import os

directory_check = '/home/analytics/Raja/test/'
if not os.path.exists(directory_check):
    os.mkdir(directory_check)

block_blob_service = BlockBlobService(account_name=' ',
                                      sas_token=" ")
generator = block_blob_service.list_blobs('tdp-reports', prefix='exhust_reports/response/venv.zip')

for blob in generator:
    x = blob.name
    print(x)
    y = x.replace('exhust_reports/response', '')
    file_path1 = directory_check + y
    print(file_path1)
    with open(file_path1, 'ab') as fp:
        b = block_blob_service.get_blob_to_bytes('tdp-reports', x)
        fp.write(b.content)
