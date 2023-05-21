# Dow Jones U.S. Banks Index

import pandas as pd

start_year = 2017
end_year = pd.Timestamp.now().year
historical_quotes = pd.DataFrame()

# frequency=daily, results limited to one year
url = "https://www.marketwatch.com/investing/index/djusbk/downloaddatapartial?startdate={}%2000:00:00&enddate={}%2000:00:00&daterange=d30&frequency=p1d&csvdownload=true&downloadpartial=false&newdates=false"
for year in range(start_year, end_year + 1):
    historical_quotes = pd.concat([
        historical_quotes,
        pd.read_csv(url.replace("startdate={}", "startdate=" + pd.Timestamp(year, 1, 1).strftime('%m/%d/%Y')).replace(
            "enddate={}", "enddate=" + pd.Timestamp(year, 12, 31).strftime('%m/%d/%Y')))
    ])

historical_quotes.insert(loc=0, column='Ticker', value='DJUSBK')
historical_quotes.to_csv("../../data/dow_jones_us_banks_index.csv", index=False)