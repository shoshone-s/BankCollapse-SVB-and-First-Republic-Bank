"""
"""
# import modules
import requests
import pandas as pd
import os
import time
import csv
import util
import aws_read_write

from dotenv import load_dotenv, dotenv_values
load_dotenv()



# TODO: check if this works... 
def transform_sec_data():
    rename_cols = {
        'end': 'end_date',
        'val': 'value',
        'accn': 'asset_num',
        'fy': 'fiscal_year',
        'fp': 'fiscal_period',
        'form': 'form',
        'filed': 'date_filed',
        'frame': 'frame',
        'start': 'start_date',
        'index': 'id'
    }

    sec_df = extract_sec_data()
    clean_sec_df = sec_df.rename(columns=rename_cols)
    clean_sec_df['end_date'] = pd.to_datetime(clean_sec_df['end_date'], format='%Y%m%d')
    clean_sec_df=clean_sec_df[~(clean_sec_df['end_date'] < '2016-12-31')]

    return clean_sec_df

def load_clean_sec_data():

    clean_data_path = 'sec_data.csv'
    clean_data_object_name='data/clean_data/sec_data.csv'
    new_clean_df = transform_sec_data()

    util.load_clean_data(new_clean_df, clean_data_path, clean_data_object_name)

def transform(table_name='all'):
    if table_name == 'all':
        load_clean_sec_data()
    elif table_name == 'sec_data':
        load_clean_sec_data()
    else:
        print('Invalid table name')
