# add balance_sheet data 

"""
    raw data sources: 
        - alpha_vantage
        - fdic
"""

import util
import aws_read_write
import pandas as pd

def transform_alpha_vantage():
    
    dest_table_name = 'financials'
    source = 'alpha_vantage'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name

    financials_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)

    return financials_df

def transform_fdic():

    dest_table_name = 'financials'
    source = 'fdic'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name

    financials_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)

    return financials_df

def transform():
    av_df = transform_alpha_vantage()
    fdic_df = transform_fdic()

    return pd.concat([av_df, fdic_df])


def load_clean_financials():

    clean_data_path = 'financials.csv'
    existing_object_name='clean_data/financials.csv'
    clean_av_stock_price = transform_financials()

    util.load_clean_data(clean_av_stock_price, clean_data_path, existing_object_name)