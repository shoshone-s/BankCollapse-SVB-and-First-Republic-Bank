import io
import boto3
import pandas as pd
import configparser

# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")

AWS_ACCESS_KEY_ID = cfg_data["AWS"]["access_key_id"]
AWS_SECRET_ACCESS_KEY = cfg_data["AWS"]["secret_access_key"]

S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]
REDSHIFT_DB_NAME = cfg_data["Redshift"]["database_name"]
REDSHIFT_WORKGROUP_NAME = cfg_data["Redshift"]["workgroup_name"]
IAM_REDSHIFT = cfg_data["Redshift"]["iam_role"]


def upload_file(file_name, bucket_name, object_name):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    response = s3_client.upload_file(file_name, bucket_name, object_name)

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 upload_file response. Status - {status}")
    else:
        print(f"Unsuccessful S3 upload_file response. Status - {status}")


def get_file(bucket_name, object_name):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_name
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")

    return response


def get_csv(bucket_name, object_name):
    response = get_file(bucket_name, object_name)

    df = pd.read_csv(response.get("Body"))
    return df


def get_parquet(bucket_name, object_name):
    response = get_file(bucket_name, object_name)

    buffer = io.BytesIO()
    s3 = boto3.resource("s3")
    s3_object = s3.Object(bucket_name, object_name)
    s3_object.download_fileobj(buffer)
    df = pd.read_parquet(buffer)
    return df