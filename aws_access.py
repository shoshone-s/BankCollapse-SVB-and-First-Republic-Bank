import boto3
import os
# from pprint import pprint
import pathlib

# connect to S3
# s3 = boto3.resource("s3")

# # display bucket names
# for bucket in s3.buckets.all():
#     print(bucket.name)


# upload file
def upload_file(object_name):
    try:
        # uses the boto3 client
        s3 = boto3.client('s3')
        bucket_name = 'ds4ateam20'
        # make sure the object name and file name match
        object_name = '/Users/naledikekana/BankCollapse-SVB-and-First-Republic-Bank/dow_jones.xls'
        file_name = "/Users/naledikekana/BankCollapse-SVB-and-First-Republic-Bank/dow_jones.xls"
        response = s3.upload_file(file_name, bucket_name, object_name)
        if response == None:
            print("Success")
        else:
            print("Failed")
    except Exception as e:
        print(e)

obj_name = "dow_jones.xls"

upload_file(obj_name)
