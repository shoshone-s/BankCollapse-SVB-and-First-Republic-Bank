# add balance_sheet data 

"""
    raw data sources: 
        - alpha_vantage
        - fdic
"""

import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "utilities"))
import util
import aws_read_write


# keep financials from Jan 2017 to Mar 2022
MIN_DATE = pd.Timestamp(2017,1,1)
MAX_DATE = pd.Timestamp(2022,3,31)


def transform_alpha_vantage():
    
    dest_table_name = 'financials'
    source = 'alpha_vantage'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name

    av_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)
    
    av_df['fiscalDateEnding'] = pd.to_datetime(av_df['fiscalDateEnding'])
    av_df = av_df[(av_df.fiscalDateEnding>=MIN_DATE) & (av_df.fiscalDateEnding<=MAX_DATE)]
    
    av_df = av_df[av_df.source=='balance sheet'][['symbol','type','fiscalDateEnding','reportedCurrency','totalAssets','totalLiabilities','totalShareholderEquity']].merge(av_df[av_df.source=='income statement'][['symbol','type','fiscalDateEnding','reportedCurrency','totalRevenue','netIncome','grossProfit','operatingIncome','ebit']], on=['symbol','type','fiscalDateEnding','reportedCurrency'], how='outer').merge(av_df[av_df.source=='cash flow'][['symbol','type','fiscalDateEnding','reportedCurrency','operatingCashflow','profitLoss']], on=['symbol','type','fiscalDateEnding','reportedCurrency'], how='outer')
    
    return av_df

def transform_fdic():

    dest_table_name = 'financials'
    source = 'fdic'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name

    fdic_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)
    
    # selected banks from FDIC
    CERT_LIST = [24735, 59017, 21761, 628, 29147, 27389, 3511, 5146, 18409, 33947, 7213, 3510, 34968, 57803]
    
    fdic_df['REPDTE'] = pd.to_datetime(fdic_df['REPDTE'])
    fdic_df = fdic_df[fdic_df.CERT.isin(CERT_LIST) & (fdic_df.REPDTE>=MIN_DATE) & (fdic_df.REPDTE<=MAX_DATE)][['CERT','REPDTE','ASSET','LIAB','ROA','ROE','EEFFR','NIMY','INTINCY']]
    fdic_df['symbol'] = fdic_df['CERT'].replace({18409:'TD', 33947:'TD', 21761:'JPM', 628:'JPM', 24735:'SIVBQ', 27389:'WFC', 3511:'WFC', 5146:'WFC', 29147:'NECB', 34968:'BPOP', 3510:'BAC', 57803:'ALLY', 59017:'FRC', 7213:'C'})

    return fdic_df

def transform_financials():
    av_df = transform_alpha_vantage()
    fdic_df = transform_fdic()
    
    av_df.rename(columns={'fiscalDateEnding':'report_date', 'reportedCurrency':'currency', 'totalAssets':'total_assets', 'totalLiabilities':'total_liabilities', 'totalShareholderEquity':'total_shareholder_equity', 'totalRevenue':'total_revenue', 'netIncome':'net_income', 'grossProfit':'gross_profit', 'operatingIncome':'operating_income', 'operatingCashflow':'operating_cashflow', 'profitLoss':'profit_loss'}, inplace=True)
    fdic_df.rename(columns={'CERT':'fdic_cert_id', 'REPDTE':'report_date', 'ASSET':'total_assets', 'LIAB':'total_liabilities', 'ROA':'return_on_assets', 'ROE':'return_on_equity', 'EEFFR':'efficiency_ratio', 'NIMY':'net_interest_margin', 'INTINCY':'yield_on_earning_assets'}, inplace=True)
    
    clean_financials = pd.concat([av_df, fdic_df])[['symbol', 'fdic_cert_id', 'report_date', 'type', 'currency', 'total_assets', 'total_liabilities', 'total_shareholder_equity', 'total_revenue', 'net_income', 'gross_profit', 'operating_income', 'ebit', 'operating_cashflow', 'profit_loss', 'return_on_assets', 'return_on_equity', 'efficiency_ratio', 'net_interest_margin', 'yield_on_earning_assets']]

    return clean_financials


def load_clean_financials():
    clean_financials = transform_financials()
    dest_table_name = 'financials'
    csv_file_name = dest_table_name + '.csv'
    util.load_clean_data(clean_financials, csv_file_name)
    
def transform(table_name='all'):
    if table_name == 'all':
        load_clean_financials()
    elif table_name == 'financials':
        load_clean_financials()
    else:
        print('Invalid table name')
