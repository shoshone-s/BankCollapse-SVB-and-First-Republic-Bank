"""
"""
import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "utilities"))
import util
import aws_read_write


def transform_sec_data():
    
    dest_table_name = 'sec_data'
    csv_file_name = dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name
    
    sec_df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name=raw_s3_object_name)
    rename_cols = {
        'end': 'end_date',
        'val': 'value',
        'accn': 'asset_num',
        'fy': 'fiscal_year',
        'fp': 'fiscal_period',
        'form': 'form',
        'filed': 'date_filed',
        'frame': 'frame',
        'start': 'start_date',
        'index': 'id'
    }
    sec_df = sec_df.rename(columns=rename_cols).assign(symbol='SIVBQ')[['id','symbol','asset_num','start_date','end_date','date_filed','fiscal_year','fiscal_period','form','frame','value']]
    
    return sec_df

def load_clean_sec_data():
    clean_sec_data = transform_sec_data()
    dest_table_name = 'sec_data'
    csv_file_name = dest_table_name + '.csv'
    util.load_clean_data(clean_sec_data, csv_file_name)

def transform(table_name='all'):
    if table_name == 'all':
        load_clean_sec_data()
    elif table_name == 'sec_data':
        load_clean_sec_data()
    else:
        print('Invalid table name')
