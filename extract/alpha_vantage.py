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
def extract_financials():
    incst = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        incst_url = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(incst_url)
        incst_data = r.json()
        incst = pd.concat([incst, pd.concat([pd.DataFrame(incst_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(incst_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])

    balsh = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        balsh_url = f'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(balsh_url)
        balsh_data = r.json()
        balsh = pd.concat([balsh, pd.concat([pd.DataFrame(balsh_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(balsh_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])


    cashflow = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        cashflow_url = f'https://www.alphavantage.co/query?function=CASH_FLOW&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(cashflow_url)
        cashflow_data = r.json()
        cashflow = pd.concat([cashflow, pd.concat([pd.DataFrame(cashflow_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(cashflow_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])

    return pd.concat([balsh, incst, cashflow]).reset_index()


def extract_price_history():
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
    csv_file_name = SOURCE_NAME + '_' + dest_table_name + '.csv'
    s3_object_name= 'raw_data/' + csv_file_name

    util.load_raw_data(companies_df, csv_file_name)

# FIXME: This data is not being transformed or to a table in the database
def load_raw_income_statement():
    incst_df = extract_companies()
    csv_file_name = "\\income_statement.csv"
    s3_object_name= 'raw_data/income_statement.csv'

    util.load_raw_data(incst_df, csv_file_name)

# FIXME: This data is not being transformed or to a table in the database
def load_raw_finacnials():
    fin_df = extract_financials()
    csv_file_name = "\\alpha_vantange_financials.csv"
    s3_object_name= 'data/raw_data/alpha_vantange_financials.csv'

    util.load_raw_data(fin_df, csv_file_name)

# FIXME: This data is not being transformed or to a table in the database
def load_raw_cashflow():
    cashflow_df = extract_cashflow()
    csv_file_name = "\\cash_flow.csv"
    s3_object_name= 'raw_data/cash_flow.csv'

    util.load_raw_data(cashflow_df, csv_file_name)

def load_raw_price_history():
    stock_prices = extract_price_history()
    dest_table_name = 'price_history'
    csv_file_name = SOURCE_NAME + "_" + dest_table_name + '.csv'
    s3_object_name= 'raw_data/' + csv_file_name 

    util.load_raw_data(stock_prices, csv_file_name)

### END OF EXTRACT METHODS ###



def extract(table_name='all'): 
    if table_name == 'all':
        load_raw_companies()
        load_raw_income_statement()
        load_raw_balance_sheet()
        load_raw_cashflow()
        load_raw_price_history()
    elif table_name == 'companies':
        load_raw_companies()
    elif table_name == 'income_statement':
        load_raw_income_statement()
    elif table_name == 'balance_sheet':
        load_raw_balance_sheet()
    elif table_name == 'cashflow':
        load_raw_cashflow()
    elif table_name == 'price_history':
        load_raw_price_history()
    else:
        print('Invalid table name')
