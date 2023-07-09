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


def load_market_watch():

    # Eventually goes to price history 
    
    historical_quotes = extract_price_history()

    # save data to csv
    historical_quotes.to_csv(data_path + "\\market_watch.csv", index=False)
    # upload data to S3 bucket
    aws_read_write.upload_file(file_name=data_path + '\\market_watch.csv', bucket_name=util.S3_BUCKET_NAME, object_name='raw_data/market_watch.csv')

### END OF EXTRACT METHODS ###

### BEGIN TRANSFORM METHODS ###

def transform_price_history(): 
    djusbank = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='raw_data/market_watch.csv')
    djusbank.columns = [x.lower() for x in djusbank.columns]
    djusbank.rename(columns={'ticker':'symbol'}, inplace=True)
    djusbank['date'] = pd.to_datetime(djusbank['date']) 
    
    # keep stock data from Jan 2017 to Mar 2022
    MIN_DATE = pd.Timestamp(2017,1,1)
    MAX_DATE = pd.Timestamp(2022,3,31)
    djusbank = djusbank[['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']]
    djusbank = djusbank[(djusbank.date>=MIN_DATE) & (djusbank.date<=MAX_DATE)]
    djusbank['volume'] = djusbank['volume'].astype('Int64')

    return djusbank

def load_clean_price_history():

    clean_data_path = 'price_history.csv'
    existing_object_name='clean_data/price_history.csv'
    djusbank = transform_price_history()

    util.load_clean_data(djusbank, clean_data_path, existing_object_name)
### END OF TRANSFORM METHODS ###
