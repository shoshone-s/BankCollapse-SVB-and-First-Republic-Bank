import boto3
import os
import pathlib


# upload file to an S3 bucket
def upload_file():
    try:
        # uses the boto3 client
        s3 = boto3.client('s3')

        # specify the S3 bucket name
        bucket_name = input('Bucket Name: ')

        # make sure the object name and file name match

        # object_name will prompt in the terminal to specify the object name uploaded to the S3 bucket
        object_name = input('Object Name: ')
        # file name is the name that you saved your file under. It will prompt for the file name
        file_name = input('File Name: ')

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


upload_file()
