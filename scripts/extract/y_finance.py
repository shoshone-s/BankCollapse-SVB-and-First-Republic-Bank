import yfinance as yf
import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "utilities"))
import util

"""
    Description : Retreives high, low, open, close, and adjusted close price, as well as volume for any stock ticker with yfinance library
"""

SOURCE_NAME = 'yfinance'
# List of stocks in scope
STOCKS_IN_SCOPE = [
    'SBNY','SIVBQ' # Silicon Valley Bank
]

def extract_price_history(): 

    df = pd.DataFrame()
    for company in STOCKS_IN_SCOPE:
        data = yf.download(company, period='max', interval='1d').reset_index()
        data.insert(loc=0, column='Ticker', value=company)
        print(len(data))
        df = pd.concat([data, df])

    # # augment so that it give the min for each date
    # print("Loading %i days worth of minute by minute data" % (len(df['Date'])))
    # day_data_lst = []
    # for bd in df['Datetime']: 
    #     day_data = yf.download(companies_str, start=bd, period='1d', interval='1m')
    #     day_data_lst.append(day_data)
    # # convert to csv
    # df = pd.concat(day_data_lst)
    
    print("Successfully extracted from yfinance: price history")
    
    return df 

def load_raw_price_history(): 
    raw_df = extract_price_history()
    dest_table_name = 'price_history'
    csv_file_name = SOURCE_NAME + '_' + dest_table_name + '.csv'
    util.load_raw_data(raw_df, csv_file_name)

### END TRANSFORM METHODS ###

def extract(table_name='all'): 
    if table_name == 'all':
        load_raw_price_history()
    elif table_name == 'price_history':
        load_raw_price_history()
    else:
        print("Invalid table name.")