"""
    Transform raw data and load clean csvs to s3 bucket
"""

import pandas as pd
import os
import configparser
import aws_read_write


# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")
S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]

TABLE_COLUMN_NAMES = {
    'company': ['id', 'name', 'symbolid', 'FDICCertID', 'Class', 'Status'],
    'debt_to_equity': ['ID', 'Date', 'LongTermDebt', 'DebtEquityRatio'],
    'financials': ['ID', 'report_date', 'total_assets', 'total_liabilities', 'total_debt', 'assets_return', 'equity_return', 'efficiency', 'risk_base_capital_ratio'],
    'location': ['ID', 'cert', 'company_name', 'main_office', 'branch_name', 'established_date', 'service_type', 'address', 'county', 'city', 'state', 'zip', 'latitude', 'longitude'],
    'price_history': ['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume'],
    'sec_data': ['ID', 'asset_num', 'company_id', 'report_type', 'start_date', 'end_date', 'date_filed', 'fiscal_year', 'fiscal_period', 'form', 'frame', 'value'],
    'symbol': ['ID', 'TickerSymbol', 'Name'],
}

# location of data files
data_path = os.path.join(os.getcwd(), "data_sources\data")

# TODO: Add all transform methods from all data sources here...

def transform_price_history():

    # combine data from Alpha Vantage, Yahoo Finance, and DJUSBK


    # keep stock data from Jan 2017 to Mar 2022
    MIN_DATE = pd.Timestamp(2017,1,1)
    MAX_DATE = pd.Timestamp(2022,3,31)
    clean_price_history = pd.concat([
        av_stock_price, 
        yf_stock_price,
        djusbank
    ])[['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']]
    clean_price_history = clean_price_history[(clean_price_history.date>=MIN_DATE) & (clean_price_history.date<=MAX_DATE)]
    clean_price_history['volume'] = clean_price_history['volume'].astype('Int64')


    # save data to csv and upload data to S3 bucket
    clean_price_history.to_csv(data_path + "\\clean_price_history.csv", index=False)
    aws_read_write.upload_file(file_name=data_path + '\\clean_price_history.csv', bucket_name=S3_BUCKET_NAME, object_name='clean_data/price_history.csv')


transform_price_history()
