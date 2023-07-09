import yfinance as yf
import pandas as pd
import aws_read_write
import util

"""
    Description : Retreives high, low, open, close, and adjusted close price, as well as volume for any stock ticker with yfinacne library
"""

# List of stocks in scope
STOCKS_IN_SCOPE = [
    'SBNY', # Silicon Valley Bank
]

# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")
S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]

def extract_price_history(): 
    if len(STOCKS_IN_SCOPE) > 1:
        companies_str = str(" ".join(STOCKS_IN_SCOPE))
    elif len(STOCKS_IN_SCOPE) == 1: 
        companies_str = STOCKS_IN_SCOPE[0]
    else: 
        companies_str = ""

    data = yf.download(companies_str, period='max', interval='1m').reset_index()

    # augment so that it give the min for each date

    print("Loading %i days worth of minute by minute data" % (len(data['Datetime'])))
    day_data_lst = []
    for bd in data['Datetime']: 
        day_data = yf.download(companies_str, start=bd, period='1d', interval='1m')
        day_data_lst.append(day_data)

    # convert to csv
    df = pd.concat(day_data_lst)
    return df 

def load_raw_price_history(): 
    raw_df = extract_price_history() 
    csv_file_name = "\\yfinance_price_history.csv"
    s3_object_name= 'raw_data/yfinance_price_history.csv'

    util.load_raw_data(raw_df, csv_file_name, s3_object_name)

#TODO : Add transform 

def transform_price_history(): 
    yf_stock_price = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='raw_data/yfinance_price_history.csv').drop(columns=['Unnamed: 0'])
    yf_stock_price.columns = [x.lower().replace(' ','_') for x in yf_stock_price.columns]
    yf_stock_price.rename(columns={'datetime':'date', 'adj_close':'adjusted_close'}, inplace=True)
    yf_stock_price['symbol'] = 'SIVBQ'
    yf_stock_price['date'] = pd.to_datetime(yf_stock_price['date']) 

    # keep stock data from Jan 2017 to Mar 2022
    MIN_DATE = pd.Timestamp(2017,1,1)
    MAX_DATE = pd.Timestamp(2022,3,31)
    yf_stock_price = yf_stock_price[['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']]
    yf_stock_price = yf_stock_price[(yf_stock_price.date>=MIN_DATE) & (yf_stock_price.date<=MAX_DATE)]
    yf_stock_price['volume'] = yf_stock_price['volume'].astype('Int64')

    return yf_stock_price

def load_clean_price_history():

    clean_data_path = 'price_history.csv'
    existing_object_name='clean_data/price_history.csv'
    clean_av_stock_price = transform_price_history()

    util.load_clean_data(clean_av_stock_price, clean_data_path, existing_object_name)


### END TRANSFORM METHODS ###


