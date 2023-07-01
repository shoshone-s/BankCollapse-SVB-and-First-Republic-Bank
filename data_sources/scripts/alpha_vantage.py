import pandas as pd
import requests
import time
import configparser


# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")
# save Alpha Vantage API key
AV_API_KEY = cfg_data["AlphaVantage"]["api_key"]


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
upload_file(file_name=data_path + '\company_overview.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/company_overview.csv')
upload_file(file_name=data_path + '\income_statement.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/income_statement.csv')
upload_file(file_name=data_path + '\balance_sheet.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/balance_sheet.csv')
upload_file(file_name=data_path + '\cash_flow.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/cash_flow.csv')
upload_file(file_name=data_path + '\stock_price_daily.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/stock_price_daily.csv')


# combine data from Alpha Vantage and Yahoo Finance

av_stock_price = get_csv(bucket_name=S3_BUCKET_NAME, object_name='raw_data/stock_price_daily.csv')
av_stock_price.columns = [x.lower().replace(' ','_') for x in av_stock_price.columns]
av_stock_price['date'] = pd.to_datetime(av_stock_price['date'])

yf_stock_price = get_csv(bucket_name=S3_BUCKET_NAME, object_name='raw_data/price_history.csv').drop(columns=['Unnamed: 0'])
yf_stock_price.columns = [x.lower().replace(' ','_') for x in yf_stock_price.columns]
yf_stock_price.rename(columns={'datetime':'date', 'adj_close':'adjusted_close'}, inplace=True)
yf_stock_price['symbol'] = 'SIVBQ'
yf_stock_price['date'] = pd.to_datetime(yf_stock_price['date'])

MIN_PRICE_YEAR = 2017
clean_price_history = pd.concat([
    av_stock_price[av_stock_price.date.dt.year>=MIN_PRICE_YEAR], 
    yf_stock_price[yf_stock_price.date.dt.year>=MIN_PRICE_YEAR]
])[['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']]


# save data to csv and upload data to S3 bucket
clean_price_history.to_csv(data_path + "\clean_price_history.csv", index=False)
upload_file(file_name=data_path + '\clean_price_history.csv', bucket_name=S3_BUCKET_NAME, object_name='transformed_data/price_history.csv')