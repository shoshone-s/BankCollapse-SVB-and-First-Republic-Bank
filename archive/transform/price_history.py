def transform_alpha_vantage():
    
    dest_table_name = 'price_history'
    source = 'alpha_vantage'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name

def transform_market_watch():
    dest_table_name = 'price_history'
    source = 'market_watch'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name

def transform_y_finance():
    dest_table_name = 'price_history'
    source = 'y_finance'
    csv_file_name = source + '_' + dest_table_name + '.csv'
    raw_s3_object_name= 'data/raw_data/' + csv_file_name