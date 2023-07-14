"""
"""
# import modules
import requests
import pandas as pd
import os
import time
import csv
import util
import aws_read_write

from dotenv import load_dotenv, dotenv_values
load_dotenv()


# DEFAULT_config=dotenv_values('.env')
DEFAULT_headers = util.cfg_data["SEC_EDGAR_USER_AGENT"]["user_agent"]


# request for basic company data
# this will return a list of dictionaries containing the following:
# - 'cik_str' - necesary to make other requests
# - 'ticker' - the company's stock ticker name
# - 'title' - name of the company
def companyTickerData(headers, companyTicker):
    # get data from all companies
    secCompanyInfo = requests.get(
        'https://www.sec.gov/files/company_tickers.json',
        headers=headers
    )
    totalCompanyData=secCompanyInfo.json()
    try:
        for companyDict in totalCompanyData.values():
            if companyDict['ticker'] == companyTicker:
                return companyDict
            else:
                pass
    except:
        return f'Sorry, no company found with ticker: {companyTicker}'
    
    return ''

# function that will query the secEDGAR 'company facts' api route for data
# this function should do the following:
# 1 - take the company ticker, find the company's cik (Central Index Key), and add leading zero's (this is needed because 
# the other routes require a 10-digit number)
# 2 - make requests to the secEDGAR 'company facts' route and pull the 'Assets', 'CommonStockValue', 
# 'CommonStockSharesIssued', 'DeferedRevenue', 'Deposits', 'IncomeTaxesPaid', 
# 'Investments', 'Liabilities', 'OtherAssets', 'OtherLiabilites', 'ProfitLoss', 'SharePrice'
# 3 - pack the target data into a data frame and write that dataframe to a csv
def secData(cik_str):
    # add in leading zeros to company cik_str
    cikNumber=str(cik_str).zfill(10)
    # init list of data/files to pull
    queryData = ['Assets', 'CommonStockValue', 
                'CommonStockSharesIssued', 
                'DeferredRevenue', 'Deposits', 
                'IncomeTaxesPaid', 'Investments', 
                'Liabilities', 'OtherAssets', 
                'OtherLiabilities', 'ProfitLoss', 'SharePrice']
    req_payload = {}

    req = requests.get(
        (
            f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cikNumber}.json'
        ),
        headers=DEFAULT_headers
    )
    # create a dictionary of the requested data
    for query in queryData:
        req_payload[query]=req.json()['facts']['us-gaap'][query]
        time.sleep(2)

    # reform the payload into a clean dict that can them be pushed into a dataframe and create the csv file from the dataframe
    targetDataDicts = [targetDict for record in req_payload for key in req_payload[record]['units'].keys() for targetDict in req_payload[record]['units'][key]]
    res_payload = pd.DataFrame.from_dict(targetDataDicts)
    res_payload.to_csv('data_sources/data/secData.csv', index=False)

    # return res_payload.to_json(orient='records')[1:-1].replace('},{', '} {')
    return targetDataDicts

def extract_sec_data():
    cfg_head = DEFAULT_headers
    ticker_symbol = 'SIVBQ'
    tickerInfo = companyTickerData(cfg_head, ticker_symbol)
    data = secData(tickerInfo['cik_str']) 

    sec_df = pd.DataFrame(data)

    return sec_df.reset_index()

def load_raw_sec_data():
    sec_data_df = extract_sec_data()
    csv_file_name = "\\sec_data.csv"
    s3_object_name= 'raw_data/sec_data.csv'

    util.load_raw_data(sec_data_df, csv_file_name)


def extract(table_name='all'):
    if table_name == 'all':
        load_raw_sec_data()
    elif table_name == 'sec_data':
        load_raw_sec_data()
    else:
        print("Invalid table name.")