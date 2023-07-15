"""
Description: Holds all constants for extract folder
"""
import configparser
import aws_read_write
import os
import pandas as pd

# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")

S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]

# save Alpha Vantage API key
AV_API_KEY = cfg_data["AlphaVantage"]["api_key"]

CLEAN_DATA_PATH = os.path.join(os.getcwd(), "data\clean_data\\")
RAW_DATA_PATH = os.path.join(os.getcwd(), "data\\raw_data\\")

def load_raw_data(raw_df, csv_file_name):
    # csv_file_name = SOURCE_NAME + "_" + dest_table_name + '.csv'

    local_csv_file_name= RAW_DATA_PATH +  csv_file_name

    s3_object_name = '/data/raw/' + csv_file_name

    raw_df.to_csv(csv_file_name, index=False)
    aws_read_write.upload_file(file_name=local_csv_file_name, bucket_name=S3_BUCKET_NAME, object_name= s3_object_name)


def load_clean_data(new_clean_df, clean_data_path, clean_data_object_name):
    # Merge existing clean price history data in s3 with new data
    existing_obj_df = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name=clean_data_object_name)
    
    new_obj_df = pd.concat([existing_obj_df, new_clean_df])
    s3_object_name = 'data/' + s3_object_name

    
    # save data to csv and upload data to S3 bucket
    new_obj_df.to_csv(clean_data_path, index=False)
    aws_read_write.upload_file(file_name=clean_data_path, bucket_name=S3_BUCKET_NAME, object_name=clean_data_object_name)
