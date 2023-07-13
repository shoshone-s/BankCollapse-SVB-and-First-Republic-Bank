# add balance_sheet data 

"""
    raw data sources: 
        - alpha_vantage
        - fdic
"""
# Get the path of the current directory
import os
import sys

current_directory = os.getcwd()
sys.path.append(current_directory)

import util
import aws_read_write
import pandas as pd

table_column_names = [
    "ID",
    "cert",
    "report_date",
    "total_assets",
    "total_liabilities",
    "total_debt",
    "assets_return",
    "equity_return",
    "efficiency",
    "risk_base_capital_ratio",
    "symbol"
]


def transform_alpha_vantage():
    
    dest_table_name = 'financials'
    source = 'alpha_vantage'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name

    financials_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)

    return financials_df

def transform_fdic():

    dest_table_name = 'financials'
    source = 'fdic'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name

    financials_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)

    institutions_subset = institutions_df[institutions_df.CERT.isin(CERT_LIST)].copy()
    institutions_subset['Status'] = institutions_subset['ACTIVE'].replace({0:'closed or not insured by FDIC', 1:'currently open and insured by the FDIC'})
    institutions_subset['Established Date'] = pd.to_datetime(institutions_subset['ESTYMD'])
    institutions_subset['Category'] = institutions_subset['CLCODE'].replace({3:'National bank', 13:'State commercial bank', 21:'State commercial bank', 41:'State chartered stock savings and co-operative bank'})
    institutions_subset['Symbol'] = list(map(lambda x: 'ALLY' if 'ally bank' in x else 'BAC' if 'bank of america' in x else 'C' if 'citibank' in x else 'JPM' if 'jpmorgan' in x else 'NECB' if 'northeast community bank' in x else 'BPOP' if 'banco popular' in x else 'TD' if 'td bank' in x else 'WFC' if 'wells fargo' in x else 'SIVB' if 'silicon valley bank' in x else 'FRC' if 'first republic bank' in x else np.nan, institutions_subset['NAME'].str.lower()))

    return financials_df

def transform():
    
    # Make a dataframce with the column names
    company_df = pd.DataFrame(columns=table_column_names)
    av_df = transform_alpha_vantage()
    fdic_df = transform_fdic()

    return pd.concat([av_df, fdic_df])


def load_clean_financials():

    clean_data_path = 'financials.csv'
    existing_object_name='clean_data/financials.csv'
    clean_av_stock_price = transform_financials()

    util.load_clean_data(clean_av_stock_price, clean_data_path, existing_object_name)

# run extract_financials() functions from extract/alpha_vantage.py and extract/fdic.py
import extract.alpha_vantage
import extract.fdic

extract.alpha_vantage.extract_financials()

# q: how can i run this file in terminal
# a: python extract/alpha_vantage.py

# q: it didn't work
# a: did you run it from the right directory?

# q: why did this error come up?: ModuleNotFoundError: No module named 'aws_read_write'

"""q: why did this error come up?: Traceback (most recent call last):
  File "/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/runpy.py", line 196, in _run_module_as_main
    return _run_code(code, main_globals, None,
  File "/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/runpy.py", line 86, in _run_code
    exec(code, run_globals)
  File "/Users/naledikekana/BankCollapse-SVB-and-First-Republic-Bank/transform/financials.py", line 9, in <module>
    import util
ModuleNotFoundError: No module named 'util'
"""
# a: did you run it from the right directory?
# q: what is the right directory?

# q: how can import util if it's not in the same directory?
# a: you can't
# q: how to i import another module that i made?
# a: you have to add the directory to the path
# q: how do i add the directory to the path?
# a: sys.path.append('path/to/directory')
