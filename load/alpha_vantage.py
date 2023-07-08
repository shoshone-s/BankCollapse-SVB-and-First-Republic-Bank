''''
Extracts the raw data from Alpha Vantage to EC2
'''

import pandas as pd
import requests
import time
import os
import configparser
import aws_read_write

from extract.alpha_vantage import *


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

def load_balsh():
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
    aws_read_write.upload_file(file_name=data_path + '\\cash_flow.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/cash_flow.csv')



