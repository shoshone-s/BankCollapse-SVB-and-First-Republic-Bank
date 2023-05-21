import boto3
import os
import pathlib


# upload file to an S3 bucket
def upload_file(bucket_name, file_name, object_name):
    try:
        # uses the boto3 client
        s3 = boto3.client('s3')

        
        file_name_path = os.path.join(pathlib.Path(
            __file__).parent.resolve(), file_name)

        # if the file name specified is wrong or not found on the system an error is generated

        response = s3.upload_file(file_name_path, bucket_name, object_name)
        if response == None:
            print("Success")
        else:
            print("Failed")
    except Exception as e:
        print(e)


# files should be specified in this order: bucket name, file name, object name
# bucket_name='bucket name', file_name='name_of_file', object_name='name_of_object'
upload_file(bucket_name='ds4ateam20', file_name='',
            object_name='')
