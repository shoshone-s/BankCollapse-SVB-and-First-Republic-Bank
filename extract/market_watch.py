import pandas as pd
import os
import configparser
import aws_read_write
import util


# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")
S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]

# location of data files
data_path = os.path.join(os.getcwd(), "data_sources\data")

SOURCE_NAME = 'market_watch'

def extract_price_history(): 
    start_year = 2017
    end_year = pd.Timestamp.now().year
    historical_quotes = pd.DataFrame()

    # Dow Jones U.S. Banks Index
    # frequency=daily, results limited to one year
    url = "https://www.marketwatch.com/investing/index/djusbk/downloaddatapartial?startdate={}%2000:00:00&enddate={}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false"
    for year in range(start_year, end_year + 1):
        historical_quotes = pd.concat([
            historical_quotes,
            pd.read_csv(url.replace("startdate={}", "startdate=" + pd.Timestamp(year, 1, 1).strftime('%m/%d/%Y')).replace(
                "enddate={}", "enddate=" + pd.Timestamp(year, 12, 31).strftime('%m/%d/%Y')))
        ])

    historical_quotes.insert(loc=0, column='Ticker', value='DJUSBK')

    return historical_quotes


def load_raw_price_history():    
    historical_quotes = extract_price_history()
    dest_table_name = 'price_history'
    csv_file_name = SOURCE_NAME + dest_table_name + '.csv'
    s3_object_name= 'raw_data/' + csv_file_name

    util.load_raw_data(historical_quotes, csv_file_name, s3_object_name)

### END OF EXTRACT METHODS ###

def extract(table_name='all'):
    if table_name == 'all':
        load_raw_price_history()
    elif table_name == 'price_history':
        load_raw_price_history()
    else:
        print("Invalid table name.")
    