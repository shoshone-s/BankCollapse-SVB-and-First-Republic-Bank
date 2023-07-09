import yfinance as yf
import pandas as pd
import aws_read_write
import util

"""
    Description : Retreives high, low, open, close, and adjusted close price, as well as volume for any stock ticker with yfinacne library
"""

def transform_price_history(): 
    yf_stock_price = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='raw_data/yfinance_price_history.csv')
    yf_stock_price = yf_stock_price.drop(columns=['Unnamed: 0'])
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


