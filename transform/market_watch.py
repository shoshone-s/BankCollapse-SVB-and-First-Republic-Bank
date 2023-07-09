import pandas as pd
import os
import configparser
import aws_read_write
import util


SOURCE_NAME = 'market_watch'


### BEGIN TRANSFORM METHODS ###

def transform_price_history(): 
    dest_table_name = 'price_history'
    csv_file_name = SOURCE_NAME + dest_table_name + '.csv'
    s3_object_name= 'raw_data/' + csv_file_name

    djusbank = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=s3_object_name)
    djusbank.columns = [x.lower() for x in djusbank.columns]
    djusbank.rename(columns={'ticker':'symbol'}, inplace=True)
    djusbank['date'] = pd.to_datetime(djusbank['date']) 
    
    # keep stock data from Jan 2017 to Mar 2022
    MIN_DATE = pd.Timestamp(2017,1,1)
    MAX_DATE = pd.Timestamp(2022,3,31)
    djusbank = djusbank[['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']]
    djusbank = djusbank[(djusbank.date>=MIN_DATE) & (djusbank.date<=MAX_DATE)]
    djusbank['volume'] = djusbank['volume'].astype('Int64')

    return djusbank

def load_clean_price_history():

    clean_data_path = 'price_history.csv'
    existing_object_name='clean_data/price_history.csv'
    djusbank = transform_price_history()

    util.load_clean_data(djusbank, clean_data_path, existing_object_name)
### END OF TRANSFORM METHODS ###

def transform(table_name='all'):
    if table_name == 'all':
        load_clean_price_history()
    elif table_name == 'price_history':
        load_clean_price_history()
    else:
        print("Invalid table name.")