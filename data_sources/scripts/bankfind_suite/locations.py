import pandas as pd
import requests
from requests.adapters import HTTPAdapter
import time

s = requests.Session()
s.mount("https://", HTTPAdapter(max_retries=10))

locations = pd.DataFrame()
total = s.get("https://banks.data.fdic.gov/api/locations").json()['totals']['count']
limit = 10000
offset = 0
while len(locations) < total:
    r = s.get("https://banks.data.fdic.gov/api/locations?sort_by=ID&offset={}&limit={}".format(offset, limit))
    locations = pd.concat([locations, pd.json_normalize(r.json()['data'])])
    offset += limit
    time.sleep(5)

locations.columns = [x.replace('data.', '') for x in locations.columns]
locations.to_csv('../../data/locations.csv', index=False)