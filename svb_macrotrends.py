import csv
from bs4 import BeautifulSoup
import urllib.request

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

# create the csv file and write rows to the output file
with open('svb_debt.csv', 'w', newline='') as scrape_output:
    csv_output = csv.writer(scrape_output)
    csv_output.writerows(rows)
