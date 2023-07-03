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

companies = pd.DataFrame()
for symbol in symbols:
    time.sleep(20)
    companies_url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={AV_API_KEY}'
    r = requests.get(companies_url)
    companies_data = r.json()
    companies = pd.concat([companies, pd.DataFrame([companies_data])])

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
    
stock_prices = pd.DataFrame()
for symbol in symbols:
    time.sleep(20)
    stocks_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize=full&apikey={AV_API_KEY}'
    r = requests.get(stocks_url)
    stocks_data = r.json()
    stock_prices = pd.concat([stock_prices, pd.DataFrame(stocks_data['Time Series (Daily)']).T.reset_index().rename(columns={'index':'date'}).assign(symbol=symbol)])
stock_prices.columns = [x.split('. ')[-1] for x in stock_prices.columns]


# save data to csv
companies.to_csv(data_path + "\company_overview.csv", index=False)
incst.to_csv(data_path + "\income_statement.csv", index=False)
balsh.to_csv(data_path + "\\balance_sheet.csv", index=False)
cashflow.to_csv(data_path + "\cash_flow.csv", index=False)
stock_prices.to_csv(data_path + "\stock_price_daily.csv", index=False)


# upload data to S3 bucket
aws_read_write.upload_file(file_name=data_path + '\company_overview.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/company_overview.csv')
aws_read_write.upload_file(file_name=data_path + '\income_statement.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/income_statement.csv')
aws_read_write.upload_file(file_name=data_path + '\balance_sheet.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/balance_sheet.csv')
aws_read_write.upload_file(file_name=data_path + '\cash_flow.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/cash_flow.csv')
aws_read_write.upload_file(file_name=data_path + '\stock_price_daily.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/stock_price_daily.csv')
