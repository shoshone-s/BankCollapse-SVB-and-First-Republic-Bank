"""
    raw source : 
    - fdic
"""

import util
import aws_read_write

def transform_fdic():
    source_name = 'fdic'
    dest_table_name = 'companies'
    csv_file_name = source_name + dest_table_name + '.csv'
    s3_object_name= 'data/raw_data/' + csv_file_name

    # TODO: Merge with Angela's changes

    institutions_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=s3_object_name)
    institutions_subset = institutions_df[institutions_df.CERT.isin(CERT_LIST)].copy()
    institutions_subset['Status'] = institutions_subset['ACTIVE'].replace({0:'closed or not insured by FDIC', 1:'currently open and insured by the FDIC'})
    institutions_subset['Established Date'] = pd.to_datetime(institutions_subset['ESTYMD'])
    institutions_subset['Category'] = institutions_subset['CLCODE'].replace({3:'National bank', 13:'State commercial bank', 21:'State commercial bank', 41:'State chartered stock savings and co-operative bank'})
    institutions_subset['Symbol'] = list(map(lambda x: 'ALLY' if 'ally bank' in x else 'BAC' if 'bank of america' in x else 'C' if 'citibank' in x else 'JPM' if 'jpmorgan' in x else 'NECB' if 'northeast community bank' in x else 'BPOP' if 'banco popular' in x else 'TD' if 'td bank' in x else 'WFC' if 'wells fargo' in x else 'SIVB' if 'silicon valley bank' in x else 'FRC' if 'first republic bank' in x else np.nan, institutions_subset['NAME'].str.lower()))

    return institutions_subset

def transform_alpha_vantage():
    source_name = 'alpha_vantage'
    dest_table_name = 'companies'
    csv_file_name = source_name + dest_table_name + '.csv'
    s3_object_name= 'data/raw_data/' + csv_file_name
    
    av_companies = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=s3_object_name)

def load_clean_companies():

    institutions_subset = transform_fdic()
    av_companies = transform_alpha_vantage()

    clean_companies = institutions_subset[['Symbol','CERT','NAME','Category','Status','Established Date']].merge(av_companies[['Symbol','Exchange','Sector']], on='Symbol', how='outer')
    clean_companies.columns = [x.lower().replace(' ','_') for x in clean_companies.columns]

    util.load_clean_data(clean_companies, 'clean_companies.csv', 'clean_data/clean_companies.csv')
