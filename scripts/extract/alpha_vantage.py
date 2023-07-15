''''
Extracts the raw data from Alpha Vantage to S3
'''

import pandas as pd
import requests
import time
import configparser
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "utilities"))
import util


# save Alpha Vantage API key
AV_API_KEY = util.AV_API_KEY

SOURCE_NAME = 'alpha_vantage'

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
    print("Successfully extracted from API: companies")
    
    return companies


def extract_price_history():
    stock_prices = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        print(symbol, end=' ')
        stocks_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize=full&apikey={AV_API_KEY}'
        r = requests.get(stocks_url)
        stocks_data = r.json()
        stock_prices = pd.concat([stock_prices, pd.DataFrame(stocks_data['Time Series (Daily)']).T.reset_index().rename(columns={'index':'date'}).assign(symbol=symbol)])
    stock_prices.columns = [x.split('. ')[-1] for x in stock_prices.columns]
    print("Successfully extracted from API: stock prices")
    
    return stock_prices


# FIXME: This data is not being transformed or to a table in the database
def extract_financials():
    
    balsh = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        balsh_url = f'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(balsh_url)
        balsh_data = r.json()
        balsh = pd.concat([balsh, pd.concat([pd.DataFrame(balsh_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(balsh_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])
    print("Successfully extracted from API: balance sheet")
    
    incst = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        incst_url = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(incst_url)
        incst_data = r.json()
        incst = pd.concat([incst, pd.concat([pd.DataFrame(incst_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(incst_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])
    print("Successfully extracted from API: income statement")

    cashflow = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        cashflow_url = f'https://www.alphavantage.co/query?function=CASH_FLOW&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(cashflow_url)
        cashflow_data = r.json()
        cashflow = pd.concat([cashflow, pd.concat([pd.DataFrame(cashflow_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(cashflow_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])
    print("Successfully extracted from API: cash flow")

    return pd.concat([balsh.assign(source='balance sheet'), 
                      incst.assign(source='income statement'),
                      cashflow.assign(source='cash flow')]).reset_index()


# upload raw data to S3 bucket

def load_raw_companies():
    companies_df = extract_companies()
    dest_table_name = 'company'
    csv_file_name = SOURCE_NAME + '_' + dest_table_name + '.csv'
    util.load_raw_data(companies_df, csv_file_name)

def load_raw_price_history():
    stock_prices = extract_price_history()
    dest_table_name = 'price_history'
    csv_file_name = SOURCE_NAME + '_' + dest_table_name + '.csv'
    util.load_raw_data(stock_prices, csv_file_name)    
    
# FIXME: This data is not being transformed or to a table in the database
def load_raw_financials():
    fin_df = extract_financials()
    dest_table_name = 'financials'
    csv_file_name = SOURCE_NAME + '_' + dest_table_name + '.csv'
    util.load_raw_data(fin_df, csv_file_name)


### END OF EXTRACT METHODS ###


def extract(table_name='all'): 
    if table_name == 'all':
        load_raw_companies()
        load_raw_price_history()
        load_raw_financials()
    elif table_name == 'company':
        load_raw_companies()
    elif table_name == 'price_history':
        load_raw_price_history()
    elif table_name == 'financials':
        load_raw_financials()
    else:
        print('Invalid table name')