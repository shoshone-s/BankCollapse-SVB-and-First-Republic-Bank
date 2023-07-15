"""
    raw data sources: 
        - alpha_vantage
        - fdic
"""

# Path: transform/financials.py
# Compare this snippet from transform/financials.py:
# """
#     raw data sources:

import util
import pandas as pd 

def transform_alpha_vantage():
    pass

def transform_fdic():
    institutions_df = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='data/raw_data/fdic_company.csv')
    institutions_subset = institutions_df[institutions_df.CERT.isin(CERT_LIST)].copy()
    institutions_subset['Status'] = institutions_subset['ACTIVE'].replace({0:'closed or not insured by FDIC', 1:'currently open and insured by the FDIC'})
    institutions_subset['Established Date'] = pd.to_datetime(institutions_subset['ESTYMD'])
    institutions_subset['Category'] = institutions_subset['CLCODE'].replace({3:'National bank', 13:'State commercial bank', 21:'State commercial bank', 41:'State chartered stock savings and co-operative bank'})
    institutions_subset['Symbol'] = list(map(lambda x: 'ALLY' if 'ally bank' in x else 'BAC' if 'bank of america' in x else 'C' if 'citibank' in x else 'JPM' if 'jpmorgan' in x else 'NECB' if 'northeast community bank' in x else 'BPOP' if 'banco popular' in x else 'TD' if 'td bank' in x else 'WFC' if 'wells fargo' in x else 'SIVB' if 'silicon valley bank' in x else 'FRC' if 'first republic bank' in x else np.nan, institutions_subset['NAME'].str.lower()))

    return institutions_subset


CERT_LIST = [24735, 59017, 21761, 628, 29147, 27389, 3511, 5146, 18409, 33947, 7213, 3510, 34968, 57803]


# combine data from FDIC and Alpha Vantage

institutions_df = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='data/raw_data/fdic_company.csv')
institutions_subset = institutions_df[institutions_df.CERT.isin(CERT_LIST)].copy()
institutions_subset['Status'] = institutions_subset['ACTIVE'].replace({0:'closed or not insured by FDIC', 1:'currently open and insured by the FDIC'})
institutions_subset['Established Date'] = pd.to_datetime(institutions_subset['ESTYMD'])
institutions_subset['Category'] = institutions_subset['CLCODE'].replace({3:'National bank', 13:'State commercial bank', 21:'State commercial bank', 41:'State chartered stock savings and co-operative bank'})

name_to_symbol = {'ally bank':'ALLY', 'bank of america':'BAC', 'citibank':'C', 'jpmorgan':'JPM', 'northeast community bank':'NECB', 'banco popular':'BPOP', 'td bank':'TD', 'wells fargo':'WFC', 'silicon valley bank':'SIVB', 'first republic bank':'FRC'}
institutions_subset['Symbol'] = institutions_subset['NAME'].str.lower.replace(name_to_symbol)
# institutions_subset['Symbol'] = list(map(lambda x: 'ALLY' if 'ally bank' in x else 'BAC' if 'bank of america' in x else 'C' if 'citibank' in x else 'JPM' if 'jpmorgan' in x else 'NECB' if 'northeast community bank' in x else 'BPOP' if 'banco popular' in x else 'TD' if 'td bank' in x else 'WFC' if 'wells fargo' in x else 'SIVB' if 'silicon valley bank' in x else 'FRC' if 'first republic bank' in x else np.nan, institutions_subset['NAME'].str.lower()))



av_companies = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='data/raw_data/alpha_vantage_company.csv')

clean_companies = institutions_subset[['Symbol','CERT','NAME','Category','Status','Established Date']].merge(av_companies[['Symbol','Exchange','Sector']], on='Symbol', how='outer')
clean_companies.columns = [x.lower().replace(' ','_') for x in clean_companies.columns]

clean_companies.to_csv(data_path + "\\clean_companies.csv", index=False)
aws_read_write.upload_file(file_name=data_path + '\\clean_companies.csv', bucket_name=S3_BUCKET_NAME, object_name='data/transformed_data/clean_companies.csv')
