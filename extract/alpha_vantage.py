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

### EXTRACT METHODS ###

def extract_companies():
    companies = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        companies_url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(companies_url)
        companies_data = r.json()
        companies = pd.concat([companies, pd.DataFrame([companies_data])])
    
    return companies

# FIXME: This data is not being transformed or to a table in the database
def extract_income_statement():
    incst = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        incst_url = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(incst_url)
        incst_data = r.json()
        incst = pd.concat([incst, pd.concat([pd.DataFrame(incst_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(incst_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])

    return incst

#FIXME: This data is not being transformed or to a table in the database
def extract_balance_sheet():
    balsh = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        balsh_url = f'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(balsh_url)
        balsh_data = r.json()
        balsh = pd.concat([balsh, pd.concat([pd.DataFrame(balsh_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(balsh_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])

    return balsh

# FIXME: This data is not being transformed or to a table in the database
def extract_cashflow():
    cashflow = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        cashflow_url = f'https://www.alphavantage.co/query?function=CASH_FLOW&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(cashflow_url)
        cashflow_data = r.json()
        cashflow = pd.concat([cashflow, pd.concat([pd.DataFrame(cashflow_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(cashflow_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])

    return cashflow

# FIXME: This data is not being transformed or to a table in the database
def extract_stock_prices():
    stock_prices = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        stocks_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize=full&apikey={AV_API_KEY}'
        r = requests.get(stocks_url)
        stocks_data = r.json()
        stock_prices = pd.concat([stock_prices, pd.DataFrame(stocks_data['Time Series (Daily)']).T.reset_index().rename(columns={'index':'date'}).assign(symbol=symbol)])
    stock_prices.columns = [x.split('. ')[-1] for x in stock_prices.columns]

    return stock_prices

# upload raw data to S3 bucket

def load_raw_companies():
    companies_df = extract_companies()

    dest_table_name = 'companies'
    csv_file_name = SOURCE_NAME + dest_table_name + '.csv'
    s3_object_name= 'raw_data/' + csv_file_name

    util.load_raw_data(companies_df, csv_file_name, s3_object_name)

# FIXME: This data is not being transformed or to a table in the database
def load_raw_income_statement():
    incst_df = extract_companies()
    csv_file_name = "\\income_statement.csv"
    s3_object_name= 'raw_data/income_statement.csv'

    util.load_raw_data(incst_df, csv_file_name, s3_object_name)

# FIXME: This data is not being transformed or to a table in the database
def load_raw_balance_sheet():
    balsh_df = extract_balance_sheet()
    csv_file_name = "\\balance_sheet.csv"
    s3_object_name= 'raw_data/balance_sheet.csv'

    util.load_raw_data(balsh_df, csv_file_name, s3_object_name)

# FIXME: This data is not being transformed or to a table in the database
def load_raw_cashflow():
    cashflow_df = extract_cashflow()
    csv_file_name = "\\cash_flow.csv"
    s3_object_name= 'raw_data/cash_flow.csv'

    util.load_raw_data(cashflow_df, csv_file_name, s3_object_name)

def load_raw_price_history():
    stock_prices = extract_stock_prices()
    dest_table_name = 'price_history'
    csv_file_name = SOURCE_NAME + "_" + dest_table_name + '.csv'
    s3_object_name= 'raw_data/' + "testingtestingtesting"+ csv_file_name 

    util.load_raw_data(stock_prices, csv_file_name, s3_object_name)

### END OF EXTRACT METHODS ###


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
    dest_table_name = 'companies'
    csv_file_name = SOURCE_NAME + dest_table_name + '.csv'
    s3_object_name= 'raw_data/' + csv_file_name

### END TRANSFORM METHODS ###

load_raw_price_history()

# q: Why do i get this error when i run this file?
# A: You need to run this file from the root directory of the project

#q: How can i access /Users/naledikekana/BankCollapse-SVB-and-First-Republic-Bank/aws_read_write.py from this file?
#A: You need to run this file from the root directory of the project

#q: how can i run this file from the root directory of the project?
#A: python extract/alpha_vantage.py

#q: that didn't work
#A: python -m extract.alpha_vantage