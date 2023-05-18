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
def upload_file():
    try:
        # uses the boto3 client
        s3 = boto3.client('s3')
        bucket_name = 'ds4ateam20'
        object_name = 'svb_debt.csv'
        file_name = os.path.join(pathlib.Path(
            __file__).parent.resolve(), 'svb_debt.csv')
        response = s3.upload_file(file_name, bucket_name, object_name)
        if response == None:
            print("Success")
        else:
            print("Failed")
    except Exception as e:
        print(e)


upload_file()
