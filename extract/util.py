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

clean_data_path = os.path.join(os.getcwd(), "data\trans_data")


def load_clean_data(new_clean_df, clean_data_path, clean_data_object_name):
    # Merge existing clean price history data in s3 with new data
    existing_price_history_df = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name=clean_data_object_name)
    
    price_history = pd.concat([existing_price_history_df, new_clean_df])
    
    # save data to csv and upload data to S3 bucket
    price_history.to_csv(clean_data_path, index=False)
    aws_read_write.upload_file(file_name=clean_data_path, bucket_name=S3_BUCKET_NAME, object_name=clean_data_object_name)
