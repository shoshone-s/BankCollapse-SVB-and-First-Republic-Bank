import csv
from bs4 import BeautifulSoup
import urllib.request
import pandas as pd
import aws_read_write
import util

### transform  methods
def transform_debt_to_equity():
    # get raw data from s3
    df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name='raw_data/macrotrends_debt_to_equity.csv')

    # Make raw data columns match DebtToEquity table

def load_clean_debt_to_equity():

    clean_data_path = 'debt_to_equity.csv'
    existing_object_name='clean_data/debt_to_equity.csv'
    clean_av_stock_price = transform_debt_to_equity()

    util.load_clean_data(clean_av_stock_price, clean_data_path, existing_object_name)

def transform(table_name='all'):
    if table_name == 'all':
        load_clean_debt_to_equity()
    elif table_name == 'debt_to_equity':
        load_clean_debt_to_equity()
    else:
        print('Invalid table name')