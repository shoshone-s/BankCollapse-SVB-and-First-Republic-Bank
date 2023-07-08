''''
Extracts the raw data from Alpha Vantage to EC2
'''

import pandas as pd
import requests
import time
import os
import configparser
import aws_read_write


# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")

S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]

# save Alpha Vantage API key
AV_API_KEY = cfg_data["AlphaVantage"]["api_key"]


# location of data files
data_path = os.path.join(os.getcwd(), "data_sources\data")


# pull data for these companies
symbols = ['JPM', 'TD', 'BAC', 'C', 'WFC', 'BPOP', 'ALLY', 'NECB']

def extract_companies():
    companies = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        companies_url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(companies_url)
        companies_data = r.json()
        companies = pd.concat([companies, pd.DataFrame([companies_data])])
    
    return companies
# TODO: rename 
def extract_income_statement():
    incst = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        incst_url = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(incst_url)
        incst_data = r.json()
        incst = pd.concat([incst, pd.concat([pd.DataFrame(incst_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(incst_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])

    return incst

def extract_balance_sheet():
    balsh = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        balsh_url = f'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(balsh_url)
        balsh_data = r.json()
        balsh = pd.concat([balsh, pd.concat([pd.DataFrame(balsh_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(balsh_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])

    return balsh

def extract_cashflow():
    cashflow = pd.DataFrame()
    for symbol in symbols:
        time.sleep(20)
        cashflow_url = f'https://www.alphavantage.co/query?function=CASH_FLOW&symbol={symbol}&apikey={AV_API_KEY}'
        r = requests.get(cashflow_url)
        cashflow_data = r.json()
        cashflow = pd.concat([cashflow, pd.concat([pd.DataFrame(cashflow_data['quarterlyReports']).assign(type='quarterly'), pd.DataFrame(cashflow_data['annualReports']).assign(type='annual')]).assign(symbol=symbol)])

    return cashflow

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


# save data to csv
# upload data to S3 bucket

def load_companies():
    companies_df = extract_companies()
    companies_df.to_csv(data_path + "\\companies.csv", index=False)
    aws_read_write.upload_file(file_name=data_path + '\\income_statement.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/income_statement.csv')

def load_income_statement():
    incst_df = extract_income_statement()
    incst_df.to_csv(data_path + "\\income_statement.csv", index=False)
    aws_read_write.upload_file(file_name=data_path + '\\income_statement.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/income_statement.csv')

def load_balance_sheet():
    balsh_df = extract_balance_sheet()
    balsh_df.to_csv(data_path + "\\balance_sheet.csv", index=False)
    aws_read_write.upload_file(file_name=data_path + '\\balance_sheet.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/balance_sheet.csv')

def load_cashflow():
    cashflow = extract_cashflow()
    cashflow.to_csv(data_path + "\\cash_flow.csv", index=False)
    aws_read_write.upload_file(file_name=data_path + '\\cash_flow.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/cash_flow.csv')

def load_stock_prices():
    stock_prices = extract_stock_prices()
    stock_prices.to_csv(data_path + "\\stock_price_daily.csv", index=False)
    aws_read_write.upload_file(file_name=data_path + '\\stock_price_daily.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/cash_flow.csv')



