''''
Extracts the raw data from Alpha Vantage to EC2
'''

import pandas as pd
import requests
import time
import os
import configparser
import aws_read_write
import util


# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")

S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]

# save Alpha Vantage API key
AV_API_KEY = cfg_data["AlphaVantage"]["api_key"]

RAW_DATA_PATHS = [
]

SOURCE_NAME = 'alpha_vantage'

# location of data files
data_path = os.path.join(os.getcwd(), "data\clean_data")


# pull data for these companies
symbols = ['JPM', 'TD', 'BAC', 'C', 'WFC', 'BPOP', 'ALLY', 'NECB']




### TRANSFORM METHODS ###

# TODO: Move transform methods to ./transform/

def transform_price_history(): 

    dest_table_name = 'price_history'
    csv_file_name = SOURCE_NAME + dest_table_name + '.csv'
    s3_object_name= 'raw_data/' + csv_file_name

    av_stock_price = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name=s3_object_name)
    av_stock_price.columns = [x.lower().replace(' ','_') for x in av_stock_price.columns]
    av_stock_price['date'] = pd.to_datetime(av_stock_price['date'])

    # keep stock data from Jan 2017 to Mar 2022
    MIN_DATE = pd.Timestamp(2017,1,1)
    MAX_DATE = pd.Timestamp(2022,3,31)
    clean_av_stock_price = av_stock_price[['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']]
    clean_av_stock_price = clean_av_stock_price[(clean_av_stock_price.date>=MIN_DATE) & (clean_av_stock_price.date<=MAX_DATE)]
    clean_av_stock_price['volume'] = clean_av_stock_price['volume'].astype('Int64')

    return clean_av_stock_price

def load_clean_price_history():

    clean_data_path = 'price_history.csv'
    existing_object_name='clean_data/price_history.csv'
    clean_av_stock_price = transform_price_history()

    util.load_clean_data(clean_av_stock_price, clean_data_path, existing_object_name)

def transform_companies():
    dest_table_name = 'companies'
    csv_file_name = SOURCE_NAME + dest_table_name + '.csv'
    s3_object_name= 'raw_data/' + csv_file_name

    av_companies = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name=s3_object_name)
    av_companies.columns = [x.lower().replace(' ','_') for x in av_companies.columns]
    av_companies.rename(columns={'symbol':'ticker'}, inplace=True)

    return av_companies

def load_clean_companies():

    clean_data_path = 'companies.csv'
    existing_object_name='clean_data/companies.csv'
    clean_av_stock_price = transform_companies()

    util.load_clean_data(clean_av_stock_price, clean_data_path, existing_object_name)


### END TRANSFORM METHODS ###

def transform(table_name='all'):
    if table_name == 'all':
        load_clean_price_history()
        load_clean_companies()
    elif table_name == 'price_history':
        load_clean_price_history()
    elif table_name == 'companies':
        load_clean_companies()
    else:
        print("Invalid table name.")