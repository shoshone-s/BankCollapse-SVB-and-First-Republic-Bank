"""
Description: Holds all constants for extract folder
"""
import configparser
import pandas as pd
from pathlib import Path
import aws_read_write

# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read(Path(__file__).resolve().parents[2] / "config" / "keys_config.cfg")
# print(cfg_data.sections())

AWS_ACCOUNT_ID = cfg_data["AWS"]["account_id"]
S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]
SQS_QUEUE_NAME = cfg_data["SQS"]["queue_name"]

AV_API_KEY = cfg_data["AlphaVantage"]["api_key"]

RAW_DATA_PATH = str(Path(__file__).resolve().parents[2] / "data" / "raw_data") + "\\"
CLEAN_DATA_PATH = str(Path(__file__).resolve().parents[2] / "data" / "clean_data") + "\\"


def load_raw_data(raw_df, csv_file_name):
    # csv_file_name = SOURCE_NAME + "_" + dest_table_name + '.csv'
    local_csv_file_name = RAW_DATA_PATH + csv_file_name
    s3_object_name = 'data/raw_data/' + csv_file_name
    raw_df.to_csv(local_csv_file_name, index=False)
    print(f"Sucessfully saved raw data to csv: {local_csv_file_name}")
    aws_read_write.upload_file(file_name=local_csv_file_name, bucket_name=S3_BUCKET_NAME, object_name= s3_object_name)
    print(f"Sucessfully saved raw data to S3: {s3_object_name} \n")
    

def load_clean_data(clean_df, csv_file_name):
    # Replace existing clean price history data in s3 with new data
    local_csv_file_name = CLEAN_DATA_PATH + csv_file_name
    s3_object_name = 'data/transformed_data/' + csv_file_name
    clean_df.to_csv(local_csv_file_name, index=False)
    print(f"Sucessfully saved clean data to csv: {local_csv_file_name}")
    aws_read_write.upload_file(file_name=local_csv_file_name, bucket_name=S3_BUCKET_NAME, object_name= s3_object_name)
    print(f"Sucessfully saved clean data to S3: {s3_object_name} \n")
 