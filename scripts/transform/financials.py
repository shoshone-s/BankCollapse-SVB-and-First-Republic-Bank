# add balance_sheet data 

"""
    raw data sources: 
        - alpha_vantage
        - fdic
"""

import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "utilities"))
import util
import aws_read_write

def transform_alpha_vantage():
    
    dest_table_name = 'financials'
    source = 'alpha_vantage'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name

    financials_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)

    ### do something
    
    return financials_df

def transform_fdic():

    dest_table_name = 'financials'
    source = 'fdic'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name

    financials_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)
    
    ### do something

    return financials_df

def transform_financials():
    av_df = transform_alpha_vantage()
    fdic_df = transform_fdic()
    
    ### do something
    clean_financials = pd.concat([av_df, fdic_df])

    return clean_financials


def load_clean_financials():
    clean_financials = transform_financials()
    dest_table_name = 'financials'
    csv_file_name = dest_table_name + '.csv'
    util.load_clean_data(clean_financials, csv_file_name)
    
def transform(table_name='all'):
    if table_name == 'all':
        load_clean_financials()
    elif table_name == 'financials':
        load_clean_financials()
    else:
        print('Invalid table name')
