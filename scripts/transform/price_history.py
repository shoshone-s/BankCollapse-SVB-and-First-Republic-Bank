import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "utilities"))
import util
import aws_read_write

def transform_alpha_vantage():
    
    dest_table_name = 'price_history'
    source = 'alpha_vantage'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name
    
    av_stock_price = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)
    av_stock_price.columns = [x.lower().replace(' ','_') for x in av_stock_price.columns]
    av_stock_price['date'] = pd.to_datetime(av_stock_price['date'])

    # keep stock data from Jan 2017 to Mar 2022
    MIN_DATE = pd.Timestamp(2017,1,1)
    MAX_DATE = pd.Timestamp(2022,3,31)
    clean_av_stock_price = av_stock_price[['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']]
    clean_av_stock_price = clean_av_stock_price[(clean_av_stock_price.date>=MIN_DATE) & (clean_av_stock_price.date<=MAX_DATE)]
    clean_av_stock_price['volume'] = clean_av_stock_price['volume'].astype('Int64')

    return clean_av_stock_price

def transform_market_watch():
    dest_table_name = 'price_history'
    source = 'market_watch'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name
    
    djusbank = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)
    djusbank.columns = [x.lower() for x in djusbank.columns]
    djusbank.rename(columns={'ticker':'symbol'}, inplace=True)
    djusbank['date'] = pd.to_datetime(djusbank['date']) 
    
    # keep stock data from Jan 2017 to Mar 2022
    MIN_DATE = pd.Timestamp(2017,1,1)
    MAX_DATE = pd.Timestamp(2022,3,31)
    djusbank = djusbank[['symbol', 'date', 'open', 'high', 'low', 'close']] # 'adjusted_close', 'volume' removed from site
    djusbank = djusbank[(djusbank.date>=MIN_DATE) & (djusbank.date<=MAX_DATE)]
    # djusbank['volume'] = djusbank['volume'].astype('Int64')

    return djusbank

def transform_y_finance():
    dest_table_name = 'price_history'
    source = 'yfinance'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name
    
    yf_stock_price = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)
    yf_stock_price.columns = [x.lower().replace(' ','_') for x in yf_stock_price.columns]
    yf_stock_price.rename(columns={'ticker':'symbol', 'adj_close':'adjusted_close'}, inplace=True)
    yf_stock_price['date'] = pd.to_datetime(yf_stock_price['date']) 

    # keep stock data from Jan 2017 to Mar 2022
    MIN_DATE = pd.Timestamp(2017,1,1)
    MAX_DATE = pd.Timestamp(2022,3,31)
    yf_stock_price = yf_stock_price[['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']]
    yf_stock_price = yf_stock_price[(yf_stock_price.date>=MIN_DATE) & (yf_stock_price.date<=MAX_DATE)]
    yf_stock_price['volume'] = yf_stock_price['volume'].astype('Int64')

    return yf_stock_price
    
def transform_price_history():
    av_df = transform_alpha_vantage()
    mw_df = transform_market_watch()
    yf_df = transform_y_finance()

    return pd.concat([av_df, mw_df, yf_df])

def load_clean_price_history():    
    clean_av_stock_price = transform_price_history()
    dest_table_name = 'price_history'
    csv_file_name = dest_table_name + '.csv'
    util.load_clean_data(clean_av_stock_price, csv_file_name)
    
    
def transform(table_name='all'):
    if table_name == 'all':
        load_clean_price_history()
    elif table_name == 'price_history':
        load_clean_price_history()
    else:
        print('Invalid table name')