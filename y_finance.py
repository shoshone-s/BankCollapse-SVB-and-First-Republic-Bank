import yfinance as yf
import pandas as pd
# https://aroussi.com/post/python-yahoo-finance -- "docs "
"""
    Description : Retreives high, low, open, close, and adjusted close price, as well as volume for any stock ticker with yfinacne library
"""

# List of stocks in scope
STOCKS_IN_SCOPE = [
    'SBNY', # Silicon Valley Bank
]

if len(STOCKS_IN_SCOPE) > 1:
    companies_str = str(" ".join(STOCKS_IN_SCOPE))
elif len(STOCKS_IN_SCOPE) == 1: 
    companies_str = STOCKS_IN_SCOPE[0]
else: 
    companies_str = ""

data = yf.download(companies_str, period='max', interval='1m').reset_index()

print(data.columns)

# augment so that it give the min for each date

print("Loading %i days worth of minute by minute data" % (len(data['Datetime'])))
day_data_lst = []
for bd in data['Datetime']: 
    day_data = yf.download(companies_str, start=bd, period='1d', interval='1m')
    day_data_lst.append(day_data)

# convert to csv
df = pd.concat(day_data_lst)
print(df.head())
df.to_csv("yfinance.csv")
