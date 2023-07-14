import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "utilities"))
import util
import aws_read_write


def transform_debt_to_equity():
    
    dest_table_name = 'debt_to_equity'
    source = 'macrotrends'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name
    
    df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)
    
    df.columns = [x.lower().replace(' ','_') for x in df.columns]
    df.rename(columns={'ticker':'symbol'}, inplace=True)
    df['date'] = pd.to_datetime(df['date']) 
    
    # separate currency and unit of measure
    for col in ['long_term_debt', 'shareholder_equity']:
        df[col+'_curr_symbol'] = df[col].str[0]
        df[col+'_uom'] = df[col].str[-1]
        df[col] = df[col].str[1:-1]
        
    df = df[['symbol', 'date', 'debt_to_equity_ratio', 'long_term_debt_curr_symbol', 'long_term_debt', 'long_term_debt_uom', 'shareholder_equity_curr_symbol', 'shareholder_equity', 'shareholder_equity_uom']]
    
    return df


def load_clean_debt_to_equity():
    df = transform_debt_to_equity()
    dest_table_name = 'debt_to_equity'
    csv_file_name = dest_table_name + '.csv'
    util.load_clean_data(df, csv_file_name)
    
    
def transform(table_name='all'):
    if table_name == 'all':
        load_clean_debt_to_equity()
    elif table_name == 'debt_to_equity':
        load_clean_debt_to_equity()
    else:
        print('Invalid table name')
