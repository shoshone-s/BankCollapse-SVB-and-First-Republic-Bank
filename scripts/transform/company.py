import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "utilities"))
import util
import aws_read_write

def transform_fdic():
    
    dest_table_name = 'company'
    source = 'fdic'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name
    
    # selected banks from FDIC
    CERT_LIST = [24735, 59017, 21761, 628, 29147, 27389, 3511, 5146, 18409, 33947, 7213, 3510, 34968, 57803]
   
    institutions_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)
    institutions_subset = institutions_df[institutions_df.CERT.isin(CERT_LIST)].copy()
    institutions_subset['Status'] = institutions_subset['ACTIVE'].replace({0:'closed or not insured by FDIC', 1:'currently open and insured by the FDIC'})
    institutions_subset['Established Date'] = pd.to_datetime(institutions_subset['ESTYMD'])
    institutions_subset['Category'] = institutions_subset['CLCODE'].replace({3:'National bank', 13:'State commercial bank', 21:'State commercial bank', 41:'State chartered stock savings and co-operative bank'})
    institutions_subset['Symbol'] = list(map(lambda x: 'ALLY' if 'ally bank' in x else 'BAC' if 'bank of america' in x else 'C' if 'citibank' in x else 'JPM' if 'jpmorgan' in x else 'NECB' if 'northeast community bank' in x else 'BPOP' if 'banco popular' in x else 'TD' if 'td bank' in x else 'WFC' if 'wells fargo' in x else 'SIVBQ' if 'silicon valley bank' in x else 'FRC' if 'first republic bank' in x else np.nan, institutions_subset['NAME'].str.lower()))
    
    return institutions_subset


def transform_alpha_vantage():
    
    dest_table_name = 'company'
    source = 'alpha_vantage'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name
    
    av_companies = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)
    
    return av_companies


def transform_company():
    fdic_df = transform_fdic()
    av_df = transform_alpha_vantage()
    
    # combine data from FDIC and Alpha Vantage

    clean_companies = fdic_df.rename(columns={'CERT':'FDIC_CERT_ID'})[['Symbol','FDIC_CERT_ID','NAME','Category','Status','Established Date']].merge(av_df[['Symbol','Exchange','Sector']], on='Symbol', how='outer')
    clean_companies.columns = [x.lower().replace(' ','_') for x in clean_companies.columns]

    return clean_companies
    

def load_clean_company():    
    clean_companies = transform_company()
    dest_table_name = 'company'
    csv_file_name = dest_table_name + '.csv'
    util.load_clean_data(clean_companies, csv_file_name)
    
    
def transform(table_name='all'):
    if table_name == 'all':
        load_clean_company()
    elif table_name == 'company':
        load_clean_company()
    else:
        print('Invalid table name')
