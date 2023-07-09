import csv
from bs4 import BeautifulSoup
import urllib.request
import pandas as pd

def extract():
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

def load():
    df = extract()
    df.to_csv('svb_debt.csv', index=False, header=False)
    aws_read_write.upload_file(file_name=data_path + '\\balance_sheet.csv', bucket_name=S3_BUCKET_NAME, object_name='raw_data/balance_sheet.csv')
