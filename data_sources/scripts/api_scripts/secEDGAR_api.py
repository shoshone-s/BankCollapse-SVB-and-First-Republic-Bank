"""
"""
# import modules
import requests
import pandas as pd
import os
from dotenv import load_dotenv, dotenv_values
load_dotenv()

config=dotenv_values('.env')
headers = {'User-Agent': os.getenv('SEC_EDGAR_USER_AGENT')}

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
        print(f'Sorry, no company found with ticker: {companyTicker}')
    
    return 

# request specific company information
# SVB data
# SIVBQ - is the ticker for SVB Financial Group
# svbTickerData=tickerData['6118']

# First Republic Bank data
# FRCB - this is the ticker for First Republic Bank
# frbTickerData=tickerData['5208']

# fill zeros for cik_str
def getCIKNum(cik_str):
    cikNumber=str(cik_str).zfill(10)
    return cikNumber

# get specific filing metadata for each
def filingMetadata(cik_str):
    # # parse out the CIK and add in the leading zeros
    # # this is because the api endpoint needs a 10 digit number and includes leading zeros
    cikNumber=getCIKNum(cik_str)
    # frbCIK=str(cik_str).zfill(10)
    filingData = requests.get(
        f'https://data.sec.gov/submissions/CIK{cikNumber}.json',
        headers=headers
    )
    # frbFilingData = requests.get(
    #     f'https://data.sec.gov/submissions/CIK{frbCIK}.json',
    #     headers=headers
    # )

    return filingData.json()


# get company facts
def companyFactsAssets(cik_str):
    cikNumber=getCIKNum(cik_str)
    companyFacts = requests.get(
        f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cikNumber}.json',
        headers=headers
    )
    # # for some reason running into the 'The specified key does not exist - 404' error when running this request
    # frbCompanyFacts = requests.get(
    #     f'https://data.sec.gov/api/xbrl/companyfacts/CIK{frbCIK}.json',
    #     headers=headers
    # )

    return companyFacts.json()['facts']['us-gaap']['Assets']


# get company concept data
def companyAssets(cik_str):
    cikNumber=getCIKNum(cik_str)
    companyConcept = requests.get(
        (
            f'https://data.sec.gov/api/xbrl/companyconcept/CIK{cikNumber}/us-gaap/Assets.json'
        ),
        headers=headers
    )

    # review data
    # companyConcept.json()['units']['USD'][0]
    # put data into dataframe
    companyAssetData = pd.DataFrame.from_dict((companyConcept.json()['units']['USD']))
    # dataframe of assets on the 10-K form
    company10_K_Assets = companyAssetData[companyAssetData.form=='10-K']
    # reset the index
    company10_K_Assets = company10_K_Assets.reset_index(drop=True)
    # dataframe of assets on the 10-Q form
    company10_Q_Assets = companyAssetData[companyAssetData.form=='10-Q']
    company10_Q_Assets = company10_Q_Assets.reset_index(drop=True)

    return companyConcept.json()['units']['USD']
    
# write dataframes to csv files  
# form10KPath=os.path.relpath('/Users/axyom7/Desktop/ds4a-capstone/data_sources/data/svb_Form-10K_Assets.csv')
# form10QPath=os.path.relpath('/Users/axyom7/Desktop/ds4a-capstone/data_sources/data/svb_Form-10Q_Assets.csv')

# svb10_K_Assets.to_csv(form10KPath, index=False)
# svb10_Q_Assets.to_csv(form10QPath, index=False)
