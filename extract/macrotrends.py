import csv
from bs4 import BeautifulSoup
import urllib.request
import pandas as pd
import aws_read_write
import util

def extract_debt_to_equity():
    # url
    urlpage = 'https://www.macrotrends.net/stocks/charts/SIVBQ/svb-financial-group/debt-equity-ratio'

    # query the web page and return the html
    page = urllib.request.urlopen(urlpage)

    # parse the html and store in variable
    parsed_page = BeautifulSoup(page, 'html.parser')

    # print(parsed_page)

    # find results within the table
    table = parsed_page.find('table', attrs={'class': 'table'})
    results = table.find_all('tr')

    # print('Number of results', len(results))

    # create and write headers to a list
    rows = []
    rows.append(['Date', 'Long Term Debt',
                'Shareholder Equity', 'Debt to Equity Ratio'])
    # print(rows)

    # loop over the results
    for result in results:
        # find all columns per result
        data = result.find_all('td')
        # check that the columns have data
        if len(data) == 0:
            continue
        # write columns to variables
        date = data[0].getText()
        long_term_debt = data[1].getText()
        shareholder_equity = data[2].getText()
        debt_to_equity_ratio = data[3].getText()

        # append the data from the column variables to build the output
        rows.append(
            [date, long_term_debt, shareholder_equity, debt_to_equity_ratio])

    # print rows to display the data from the list
    # print(rows)

    # Make rows a dataframe
    df = pd.DataFrame(rows)

    return df

def load_raw_debt_to_equity():
    mt_df = extract_debt_to_equity()
    csv_file_name = "macrotrends_debt_to_equity.csv"
    s3_object_name= 'raw_data/macrotrends_debt_to_equity.csv'

    util.load_raw_data(mt_df, csv_file_name, s3_object_name)


### transform  methods
def transform_debt_to_equity():
    # get raw data from s3
    df = aws_read_write.get_csv(bucket_name=util.S3_BUCKET_NAME, object_name='raw_data/macrotrends_debt_to_equity.csv')

    # Make raw data columns match DebtToEquity table

def load_clean_debt_to_equity():

    clean_data_path = 'debt_to_equity.csv'
    existing_object_name='clean_data/debt_to_equity.csv'
    clean_av_stock_price = transform_debt_to_equity()

    util.load_clean_data(clean_av_stock_price, clean_data_path, existing_object_name)

def extract(table_name='all'):
    if table_name == 'all':
        load_raw_debt_to_equity()
    elif table_name == 'debt_to_equity':
        load_raw_debt_to_equity()
    else:
        print('Invalid table name')