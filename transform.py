import pandas as pd
import numpy as np
import os
import configparser
import aws_read_write


# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")
S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]


# location of data files
data_path = os.path.join(os.getcwd(), "data_sources\data")


# selected banks from FDIC
CERT_LIST = [24735, 59017, 21761, 628, 29147, 27389, 3511, 5146, 18409, 33947, 7213, 3510, 34968, 57803]


# combine data from FDIC and Alpha Vantage

institutions_df = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='data/raw_data/institutions.csv')
institutions_subset = institutions_df[institutions_df.CERT.isin(CERT_LIST)].copy()
institutions_subset['Status'] = institutions_subset['ACTIVE'].replace({0:'closed or not insured by FDIC', 1:'currently open and insured by the FDIC'})
institutions_subset['Established Date'] = pd.to_datetime(institutions_subset['ESTYMD'])
institutions_subset['Category'] = institutions_subset['CLCODE'].replace({3:'National bank', 13:'State commercial bank', 21:'State commercial bank', 41:'State chartered stock savings and co-operative bank'})
institutions_subset['Symbol'] = list(map(lambda x: 'ALLY' if 'ally bank' in x else 'BAC' if 'bank of america' in x else 'C' if 'citibank' in x else 'JPM' if 'jpmorgan' in x else 'NECB' if 'northeast community bank' in x else 'BPOP' if 'banco popular' in x else 'TD' if 'td bank' in x else 'WFC' if 'wells fargo' in x else 'SIVB' if 'silicon valley bank' in x else 'FRC' if 'first republic bank' in x else np.nan, institutions_subset['NAME'].str.lower()))

av_companies = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='data/raw_data/company_overview.csv')

clean_companies = institutions_subset[['Symbol','CERT','NAME','Category','Status','Established Date']].merge(av_companies[['Symbol','Exchange','Sector']], on='Symbol', how='outer')
clean_companies.columns = [x.lower().replace(' ','_') for x in clean_companies.columns]

clean_companies.to_csv(data_path + "\\clean_companies.csv", index=False)
aws_read_write.upload_file(file_name=data_path + '\\clean_companies.csv', bucket_name=S3_BUCKET_NAME, object_name='data/transformed_data/clean_companies.csv')



locations_df = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='data/raw_data/locations.csv')

clean_locations = locations_df[locations_df.CERT.isin(CERT_LIST)].sort_values('NAME')[['CERT','NAME','MAINOFF','OFFNAME','ESTYMD','SERVTYPE','ADDRESS','COUNTY','CITY','STNAME','ZIP','LATITUDE','LONGITUDE']].rename(columns={'NAME':'company_name', 'MAINOFF':'main_office', 'OFFNAME':'branch_name', 'ESTYMD':'established_date', 'SERVTYPE':'service_type', 'STNAME':'state'})
clean_locations.columns = [x.lower() for x in clean_locations.columns]
clean_locations['established_date'] = pd.to_datetime(clean_locations['established_date'])
clean_locations['service_type'] = clean_locations['service_type'].replace({11:'Full Service Brick and Mortar Office', 12:'Full Service Retail Office', 13:'Full Service Cyber Office', 14:'Full Service Mobile Office', 15:'Full Service Home/Phone Banking', 16:'Full Service Seasonal Office', 21:'Limited Service Administrative Office', 22:'Limited Service Military Facility', 23:'Limited Service Facility Office', 24:'Limited Service Loan Production Office', 25:'Limited Service Consumer Credit Office', 26:'Limited Service Contractual Office', 27:'Limited Service Messenger Office', 28:'Limited Service Retail Office', 29:'Limited Service Mobile Office', 30:'Limited Service Trust Office'})
clean_locations['zip'] = clean_locations['zip'].astype(str)
clean_locations.loc[~clean_locations.state.isin(['Puerto Rico','Virgin Islands Of The U.S.']) & clean_locations.zip.apply(lambda x: len(x)!=5), 'zip'] = clean_locations['zip'].str.zfill(5)

clean_locations.to_csv(data_path + "\\clean_locations.csv", index=False)
aws_read_write.upload_file(file_name=data_path + '\\clean_locations.csv', bucket_name=S3_BUCKET_NAME, object_name='data/transformed_data/locations.csv')


# combine data from Alpha Vantage, Yahoo Finance, and DJUSBK

av_stock_price = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='data/raw_data/stock_price_daily.csv')
av_stock_price.columns = [x.lower().replace(' ','_') for x in av_stock_price.columns]
av_stock_price['date'] = pd.to_datetime(av_stock_price['date'])

yf_stock_price = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='data/raw_data/price_history.csv').drop(columns=['Unnamed: 0'])
yf_stock_price.columns = [x.lower().replace(' ','_') for x in yf_stock_price.columns]
yf_stock_price.rename(columns={'datetime':'date', 'adj_close':'adjusted_close'}, inplace=True)
yf_stock_price['symbol'] = 'SIVB'
yf_stock_price['date'] = pd.to_datetime(yf_stock_price['date'])

djusbank = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='data/raw_data/dow_jones_us_banks_index.csv')
djusbank.columns = [x.lower() for x in djusbank.columns]
djusbank.rename(columns={'ticker':'symbol'}, inplace=True)
djusbank['date'] = pd.to_datetime(djusbank['date'])

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
aws_read_write.upload_file(file_name=data_path + '\\clean_price_history.csv', bucket_name=S3_BUCKET_NAME, object_name='data/transformed_data/price_history.csv')