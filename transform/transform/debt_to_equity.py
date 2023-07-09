"""
    raw data sources: 
        - macrotrends
"""

import util
import aws_read_write

### transform  methods
def transform_debt_to_equity():
    # get raw data from s3
    df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name='raw_data/macrotrends_debt_to_equity.csv')

    # Make raw data columns match DebtToEquity table

def load_clean():

    clean_data_path = 'debt_to_equity.csv'
    existing_object_name='clean_data/debt_to_equity.csv'
    clean_av_stock_price = transform_debt_to_equity()

    util.load_clean_data(clean_av_stock_price, clean_data_path, existing_object_name)